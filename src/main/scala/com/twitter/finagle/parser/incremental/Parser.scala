package com.twitter.finagle.parser.incremental

import scala.collection.mutable.{ListBuffer, WrappedArray}
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util._

/**
 * The base class for all incremental ChannelBuffer parsers.
 *
 * Incremental parsers are designed to process Netty ChannelBuffers
 * that may contain only partial messages. The API is inspired by
 * existing parser combinator libraries like Haskell's Parsec and
 * Scala's built in parser combinators. In the case where the input
 * buffer cannot satisfy a parser, it will return a continuation that
 * saves progess and can be used to finish parsing once more input is
 * received.
 *
 * As an example, here is a full parser of Redis's reply protocol:
 *
 * {{{
 * import com.twitter.finagle.parser.incremental.Parser._
 *
 * trait Reply
 *
 * object Reply {
 *   case class Status(msg: ChannelBuffer) extends Reply
 *   case class Error(msg: ChannelBuffer) extends Reply
 *   case class Integer(i: Int) extends Reply
 *   case class Bulk(bytes: ChannelBuffer) extends Reply
 *   case class MultiBulk(bulks: Option[Seq[Bulk]]) extends Reply
 * }
 *
 * val CRLF = acceptString("\r\n")
 *
 * val readLine = readTo("\r\n")
 *
 * val readStatusReply = "+" then readLine map { Reply.Status(_) }
 *
 * val readErrorReply = "-" then readLine map { Reply.Error(_) }
 *
 * val readIntegerReply = ":" then readInt map { Reply.Integer(_) }
 *
 * val readBulkReply = "$" then readInt flatMap { count =>
 *   readBytes(count)
 * } thenSkip CRLF map { Reply.Bulk(_) }
 *
 * val readMultiBulkReply = "*" then readInt flatMap { count =>
 *   if (count == -1) success(null) else repN(count, readBulkReply)
 * } map { bulks =>
 *   Reply.MultiBulk(Option(bulks))
 * }
 *
 * val readReply: Parser[Reply] = {
 *   readStatusReply    or
 *   readIntegerReply   or
 *   readBulkReply      or
 *   readMultiBulkReply or
 *   readErrorReply
 * }
 * }}}
 *
 * The parser `readInt`, used above, is a good example of how one can
 * drop down to manipulating the raw input ChannelBuffer as a sort of
 * escape hatch:
 *
 * {{{
 * val readInt = withRawBuffer { decodeInt(_) }
 *
 * def decodeInt(buf: ChannelBuffer): Int = {
 *   var sign   = 1
 *   var result = 0
 *
 *   var c = buf.readByte
 *
 *   if (c == '-') {
 *     sign   = -1
 *   } else {
 *     result = result * 10 + (c - '0')
 *   }
 *
 *   do {
 *     c = buf.readByte
 *
 *     if (c != '\r') {
 *       result = result * 10 + (c - '0')
 *     } else {
 *       buf.readByte // \n
 *       return result * sign
 *     }
 *   } while (true)
 *
 *   // not reached
 *   0
 * }
 * }}}
 *
 */
abstract class Parser[+Out] {
  import Parser._

  def decode(buffer: ChannelBuffer): ParseResult[Out] = {
    try Return(decodeRaw(buffer)) catch {
      case c: Continue[_] => c.asInstanceOf[Continue[Out]]
      case f: Fail => f
      case e: Error => e
    }
  }

  def decodeRaw(buffer: ChannelBuffer): Out

  def toFrameDecoder = new ParserDecoder(this)


  // basic composition

  def flatMap[T](f: Out => Parser[T]): Parser[T] = new FlatMap1Parser(this, f)

  def map[T](f: Out => T): Parser[T] = this flatMap { rv => success(f(rv)) }

  def then[T](rhs: Parser[T]): Parser[T] = this flatMap { _ => rhs }

  def then[T](rv: T): Parser[T] = this then success(rv)

  def thenSkip(rhs: Parser[_]): Parser[Out] = this flatMap { rv => new ConstParser(rhs, rv) }

  def and[T, C <: ChainableTuple](rhs: Parser[T])(implicit chn: Out => C): Parser[C#Next[T]] = {
    for (tup <- this; next <- rhs) yield chn(tup).append(next)
  }

  def or[O >: Out](rhs: Parser[O]): Parser[O] = new OrParser(this, rhs)



  // yay operators...this may be a bad idea.

  def * = rep(this)

  def + = rep1(this)

  def ? = opt(this)

  def <<[T](rhs: Parser[T]) = this thenSkip rhs

  def >>[T](rhs: Parser[T]) = this then rhs

  def >>=[T](f: Out => Parser[T]) = this flatMap f

  def ^[T](r: T) = this then r

  def ^^[T](f: Out => T) = this map f

  def |[T](rhs: Parser[T]) = this or rhs

  def &[T, C <: ChainableTuple](rhs: Parser[T])(implicit c: Out => C): Parser[C#Next[T]] = {
    this and rhs
  }

}

final class ReturnParser[+Out](rv: Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer) = rv
}

final class FailParser(message: => String) extends Parser[Nothing] {
  def decodeRaw(buffer: ChannelBuffer) = throw Fail(() => message)
}

final class ErrorParser(message: => String) extends Parser[Nothing] {
  def decodeRaw(buffer: ChannelBuffer) = throw Error(() => message)
}

final class RawBufferParser[+Out](f: ChannelBuffer => Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val start = buffer.readerIndex
    try f(buffer) catch {
      case e: IndexOutOfBoundsException =>
        buffer.readerIndex(start)
        throw Continue(this)
    }
  }
}

final class FlatMap1Parser[A, +Out](parser: Parser[A], f1: A => Parser[Out]) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next1 = try f1(parser.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap1Parser(rest.asInstanceOf[Parser[A]], f1))
    }

    next1.decodeRaw(buffer)
  }

  override def flatMap[T](f2: Out => Parser[T]): Parser[T] = new FlatMap2Parser(parser, f1, f2)
}

final class FlatMap2Parser[A, B, +Out](
  parser: Parser[A],
  f1: A => Parser[B],
  f2: B => Parser[Out]
) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next1 = try f1(parser.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap2Parser(rest.asInstanceOf[Parser[A]], f1, f2))
    }

    val next2 = try f2(next1.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap1Parser(rest.asInstanceOf[Parser[B]], f2))
    }

    next2.decodeRaw(buffer)
  }

  override def flatMap[T](f3: Out => Parser[T]): Parser[T] = new FlatMap3Parser(parser, f1, f2, f3)
}

final class FlatMap3Parser[A, B, C, +Out](
  parser: Parser[A],
  f1: A => Parser[B],
  f2: B => Parser[C],
  f3: C => Parser[Out]
) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next1 = try f1(parser.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap3Parser(rest.asInstanceOf[Parser[A]], f1, f2, f3))
    }

    val next2 = try f2(next1.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap2Parser(rest.asInstanceOf[Parser[B]], f2, f3))
    }

    val next3 = try f3(next2.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap1Parser(rest.asInstanceOf[Parser[C]], f3))
    }

    next3.decodeRaw(buffer)
  }

  override def flatMap[T](f4: Out => Parser[T]): Parser[T] = new FlatMap4Parser(parser, f1, f2, f3, f4)
}

final class FlatMap4Parser[A, B, C, D, +Out](
  parser: Parser[A],
  f1: A => Parser[B],
  f2: B => Parser[C],
  f3: C => Parser[D],
  f4: D => Parser[Out]
) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next1 = try f1(parser.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap4Parser(rest.asInstanceOf[Parser[A]], f1, f2, f3, f4))
    }

    val next2 = try f2(next1.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap3Parser(rest.asInstanceOf[Parser[B]], f2, f3, f4))
    }

    val next3 = try f3(next2.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap2Parser(rest.asInstanceOf[Parser[C]], f3, f4))
    }

    val next4 = try f4(next3.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap1Parser(rest.asInstanceOf[Parser[D]], f4))
    }

    next4.decodeRaw(buffer)
  }

  override def flatMap[T](f5: Out => Parser[T]): Parser[T] = new FlatMap5Parser(parser, f1, f2, f3, f4, f5)
}

final class FlatMap5Parser[A, B, C, D, E, +Out](
  parser: Parser[A],
  f1: A => Parser[B],
  f2: B => Parser[C],
  f3: C => Parser[D],
  f4: D => Parser[E],
  f5: E => Parser[Out]
) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next1 = try f1(parser.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap5Parser(rest.asInstanceOf[Parser[A]], f1, f2, f3, f4, f5))
    }

    val next2 = try f2(next1.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap4Parser(rest.asInstanceOf[Parser[B]], f2, f3, f4, f5))
    }

    val next3 = try f3(next2.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap3Parser(rest.asInstanceOf[Parser[C]], f3, f4, f5))
    }

    val next4 = try f4(next3.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap2Parser(rest.asInstanceOf[Parser[D]], f4, f5))
    }

    val next5 = try f5(next4.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMap1Parser(rest.asInstanceOf[Parser[E]], f5))
    }

    next5.decodeRaw(buffer)
  }
}

final class ConstParser[+Out](parser: Parser[_], value: Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    try parser.decodeRaw(buffer) catch {
      case Continue(rest) => throw Continue(new ConstParser(parser, value))
    }

    value
  }
}

final class OrParser[+Out](choice: Parser[Out], next: Parser[Out], committed: Boolean)
extends Parser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  def decodeRaw(buffer: ChannelBuffer): Out = {
    val start = buffer.readerIndex

    try choice.decodeRaw(buffer) catch {
      case f: Fail =>
        if (committed || buffer.readerIndex > start) throw Error(f.messageString)
        next.decodeRaw(buffer)
      case Continue(rest) =>
        throw Continue(new OrParser(rest, next, committed || buffer.readerIndex > start))
    }
  }

  override def or[O >: Out](other: Parser[O]): Parser[O] = {
    new OrParser(choice, next or other)
  }
}

final class NotParser(parser: Parser[_]) extends Parser[Unit] {

  def decodeRaw(buffer: ChannelBuffer): Unit = {
    val start = buffer.readerIndex
    var succeeded = false

    try {
      parser.decodeRaw(buffer)
      succeeded = true
    } catch {
      case Continue(rest) =>
        if (buffer.readerIndex > start) {
          throw Error(() => "Expected "+ parser +" to fail, but already consumed data.")
        } else {
          throw Continue(new NotParser(rest))
        }
      case f: Fail => ()
    }

    if (succeeded) {
      if (buffer.readerIndex > start) {
        throw Error(() => "Expected "+ parser +" to fail, but already consumed data.")
      } else {
        throw Fail(() => "Expected "+ parser +" to fail.")
      }
    }
  }
}

final class RepeatTimesParser[+Out](
  parser: Parser[Out],
  total: Int,
  prevResult: Array[Any] = null,
  prevI: Int = 0,
  currParser: Parser[Out] = null
) extends Parser[Seq[Out]] {

  def decodeRaw(buffer: ChannelBuffer): Seq[Out] = {
    var i = prevI
    val result = if (prevResult ne null) prevResult else new Array[Any](total)

    try {
      if (currParser ne null) {
        val item = currParser.decodeRaw(buffer)
        result(i) = item
        i += 1
      }

      while (i < total) {
        val item = parser.decodeRaw(buffer)
        result(i) = item
        i += 1
      }
    } catch {
      case Continue(rest) => throw Continue(new RepeatTimesParser(parser, total, result, i, rest))
    }

    result.toSeq.asInstanceOf[Seq[Out]]
  }
}

final class RepeatParser[+Out](
  parser: Parser[Out],
  prevResult: ListBuffer[Out] = null,
  currParser: Parser[Out] = null
) extends Parser[Seq[Out]] {
  def decodeRaw(buffer: ChannelBuffer): Seq[Out] = {
    val result = if (prevResult ne null) prevResult else ListBuffer[Out]()

    try {
      if (currParser ne null) {
        result += currParser.decodeRaw(buffer)
      }

    do {
      result += parser.decodeRaw(buffer)
    } while (true)
    } catch {
      case Continue(rest) => throw Continue(new RepeatParser[Out](parser, result, rest.asInstanceOf[Parser[Out]]))
      case f: Fail => ()
    }

    result
  }
}


object Parser {

  // lifting values

  def fail(message: String) = new FailParser(message)

  def error(message: String) = new ErrorParser(message)

  def success[T](t: T) = new ReturnParser(t)

  val unit = success(())

  def liftOpt[T](o: Option[T]): Parser[T] = o match {
    case Some(r) => success(r)
    case None    => fail("Parse failed.")
  }


  // behavior transformation

  def opt[T](p: Parser[T]): Parser[Option[T]] = p map { Some(_) } or success(None)

  def attempt[T](p: Parser[T]) = new BacktrackingParser(p)


  // repetition

  def rep[T](p: Parser[T]): Parser[Seq[T]] = new RepeatParser(p)

  def rep1[T](p: Parser[T], q: Parser[T]): Parser[Seq[T]] = {
    val getRest = rep(q)

    for (head <- p; tail <- getRest) yield (head +: tail)
  }

  def rep1[T](p: Parser[T]): Parser[Seq[T]] = {
    rep1(p, p)
  }

  def rep1sep[T](p: Parser[T], sep: Parser[Any]): Parser[Seq[T]] = {
    rep1(p, sep then p)
  }

  def repsep[T](p: Parser[T], sep: Parser[Any]): Parser[Seq[T]] = {
    rep1sep(p, sep) or success[Seq[T]](Nil)
  }

  def repN[T](total: Int, parser: Parser[T]): Parser[Seq[T]] = {
    new RepeatTimesParser(parser, total)
  }


  // matching parsers

  def accept(m: Matcher) = new MatchParser(m) flatMap { readBytes(_) }

  implicit def acceptString(choice: String) = {
    val bytes = choice.getBytes("US-ASCII")
    new MatchParser(new DelimiterMatcher(bytes)) then skipBytes(bytes.size)
  }

  def accept(choice: String): Parser[ChannelBuffer] = {
    accept(new DelimiterMatcher(choice))
    // val m = new MatchParser(new DelimiterMatcher(choice))
    // m then skipBytes(choice.size) then success(choice)
  }

  def accept(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    accept(AlternateMatcher(first +: second +: rest))
  }

  def guard(m: Matcher) = new MatchParser(m)

  def guard(choice: String): Parser[Int] = {
    guard(new DelimiterMatcher(choice))
  }

  def guard(first: String, second: String, rest: String*): Parser[Int] = {
    guard(AlternateMatcher(first +: second +: rest))
  }

  def not(m: Matcher) = guard(m.negate) then unit

  def not(choice: String): Parser[Unit] = {
    not(new DelimiterMatcher(choice))
  }

  def not(first: String, second: String, rest: String*): Parser[Unit] = {
    not(AlternateMatcher(first +: second +: rest))
  }


  def choice[T](choices: (String, Parser[T])*): Parser[T] = {
    val (m, p)           = choices.head
    val first: Parser[T] = accept(m) then p
    val rest             = choices.tail

    if (rest.isEmpty) first else first or choice(rest: _*)
  }


  // Delimiter parsers

  def readTo(m: Matcher) = new ConsumingDelimiterParser(m)

  def readTo(choice: String): Parser[ChannelBuffer] = {
    readTo(new DelimiterMatcher(choice))
  }

  def readTo(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    readTo(AlternateMatcher(first +: second +: rest))
  }

  def bytesBefore(s: String) = {
    new DelimiterFinderParser(new DelimiterMatcher(s.getBytes("US-ASCII")))
  }

  def readUntil(m: Matcher) = new DelimiterParser(m)

  def readUntil(choice: String): Parser[ChannelBuffer] = {
    readUntil(new DelimiterMatcher(choice))
  }

  def readUntil(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    readUntil(AlternateMatcher(first +: second +: rest))
  }

  def readWhile(m: Matcher) = readUntil(new NotMatcher(m))

  def readWhile(choice: String): Parser[ChannelBuffer] = {
    readWhile(new DelimiterMatcher(choice))
  }

  def readWhile(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    readWhile(AlternateMatcher(first +: second +: rest))
  }

  val readLine = readTo(Matchers.CRLF)
  val readWord = readUntil(Matchers.WhiteSpace)


  // basic reading parsers

  def withRawBuffer[T](f: ChannelBuffer => T) = new RawBufferParser(f)

  def readBytes(count: Int) = new BytesParser(count)

  def skipBytes(count: Int) = new SkipBytesParser(count)


  // integral primitives

  val readByte = withRawBuffer { _.readByte }

  val readShort = withRawBuffer { _.readShort }

  val readMedium = withRawBuffer { _.readMedium }

  val readInt = withRawBuffer { _.readInt }

  val readLong = withRawBuffer { _.readLong }

  // Unsigned integral primitives

  val readUnsignedByte = withRawBuffer { _.readUnsignedByte }

  val readUnsignedShort = withRawBuffer { _.readUnsignedShort }

  val readUnsignedMedium = withRawBuffer { _.readUnsignedMedium }

  val readUnsignedInt = withRawBuffer { _.readUnsignedInt }

  // non-integral primitives

  val readChar = withRawBuffer { _.readChar }

  val readDouble = withRawBuffer { _.readDouble }

  val readFloat = withRawBuffer { _.readFloat }
}
