package com.twitter.finagle.parser.incremental

import scala.collection.mutable.{ListBuffer, WrappedArray}
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util._

/**
 * The base class for all incremental ChannelBuffer parsers.
 *
 * @author Matt Freels
 *
 * Incremental parsers are designed to process Netty ChannelBuffers
 * that may contain only partial messages. The API is inspired by
 * existing parser combinator libraries like Haskell's Parsec and
 * Scala's built in parser combinators. In the case where the input
 * buffer cannot satisfy a parser, it will return a continuation that
 * saves progess and can be used to finish parsing once more input is
 * received.
 *
 * Like Parsec, incremental parsers are predictive, in the sense that
 * if they consume any data, they are expected to succeed. If a parser
 * Fails, it has not consumed any data. If a parser Errors, then it
 * has consumed data, and is not normally recoverable. The notable
 * exception is that a parser that can potentially Error can be made
 * one that Fails by wrapping it with `attempt`.
 *
 * Example:
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
 *   case class Bulk(bytes: Option[ChannelBuffer]) extends Reply
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
 *   if (count == -1) success(null) else readBytes(count)
 * } thenSkip CRLF map { b => Reply.Bulk(Option(b)) }
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

  /**
   * Parse a `ChannelBuffer` and return a result, which is one of `Return`,
   * `Fail`, `Error`, or `Continue`. Return, Error, and Fail are final
   * results. Continue indicates that there was insufficient data to
   * complete parsing, and returns an intermediate parser that should be
   * used to continue parsing once more data is available.
   */
  def decode(buffer: ChannelBuffer): ParseResult[Out] = {
    try Return(decodeRaw(buffer)) catch {
      case c: Continue[_] => c.asInstanceOf[Continue[Out]]
      case f: Fail => f
      case e: Error => e
    }
  }

  /**
   * Parse a `ChannelBuffer` and directly return the output. Throws a
   * `ParseException` in the case parsing fails.
   */
  @throws(classOf[ParseException])
  def decodeRaw(buffer: ChannelBuffer): Out

  /**
   * Return a Netty `FrameDecoder` that can be used in a `ChannelPipeline`.
   */
  def toFrameDecoder = new ParserDecoder(this)


  /* Basic Composition */

  /**
   * Returns a parser that applies this and continues with the
   * parser returned by applying `f` to the intermediate result.
   */
  def flatMap[T](f: Out => Parser[T]): Parser[T] = new FlatMap1Parser(this, f)

  /**
   * Returns a parser that applies this and continues with the
   * parser returned by applying `f` to the intermediate result.
   */
  def andThen[T](f: Out => Parser[T]): Parser[T] = this flatMap f

  /**
   * Returns a parser that applies `f` to the result of this.
   */
  def map[T](f: Out => T): Parser[T] = this flatMap { rv => success(f(rv)) }

  /**
   * Returns a parser that discards the result of this and returns the result of `next`.
   */
  def then[T](next: Parser[T]): Parser[T] = this flatMap { _ => next }

  /**
   * Returns a parser that discards the result of this and returns `value`.
   */
  def then[T](value: T): Parser[T] = new ConstParser(this, value)

  /**
   * Returns a parser that discards the result of `skipped` and returns the result of parsing this.
   */
  def thenSkip(skipped: Parser[_]): Parser[Out] = this flatMap { rv => skipped then rv }

  def and[T, C <: ChainableTuple](rhs: Parser[T])(implicit chn: Out => C): Parser[C#Next[T]] = {
    for (tup <- this; next <- rhs) yield chn(tup).append(next)
  }

  /**
   * Returns a parser that attempts this and applies `next` if this fails.
   */
  def or[O >: Out](next: Parser[O]): Parser[O] = new OrParser(this, next)


  // yay operators...this may be a bad idea.

  /**
   * Parse this zero or more times
   */
  def * = rep(this)

  /**
   * Parse this one or more times.
   */
  def + = rep1(this)

  /**
   * Parse this zero or one time.
   */
  def ? = opt(this)

  /**
   * Equivalent to `this thenSkip next`
   */
  def <<[T](next: Parser[T]) = this thenSkip next

  /**
   * Equivalent to `this then next`
   */
  def >>[T](next: Parser[T]) = this then next

  /**
   * Equivalent to `this flatMap next`
   */
  def >>=[T](f: Out => Parser[T]) = this flatMap f

  /**
   * Equivalent to `this then value`
   */
  def ^[T](value: T) = this then value

  /**
   * Equivalent to `this map f
   */
  def ^^[T](f: Out => T) = this map f

  /**
   * Equivalent to `this or next`
   */
  def |[T](next: Parser[T]) = this or next

  /**
   * Equivalent to `this and next`
   */
  def &[T, C <: ChainableTuple](next: Parser[T])(implicit c: Out => C): Parser[C#Next[T]] = {
    this and next
  }
}

final class ReturnParser[+Out](value: Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer) = value
}

final class FailParser(message: => String) extends Parser[Nothing] {
  def decodeRaw(buffer: ChannelBuffer) = throw Fail(() => message)
}

final class ErrorParser(message: => String) extends Parser[Nothing] {
  def decodeRaw(buffer: ChannelBuffer) = throw Error(() => message)
}

final class RawBufferParser[+Out](f: ChannelBuffer => Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    try f(buffer) catch {
      case e: IndexOutOfBoundsException => throw Continue(this)
    }
  }
}

final class RepeatingRawBufferParser[+Out](f: ChannelBuffer => Out) extends Parser[Out] {
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

  /* Lifting Values */

  /**
   * A parser that consumes no data and fails with `message`.
   */
  def fail(message: String) = new FailParser(message)

  /**
   * A parser that consumes no data and raises with `message`.
   */
  def error(message: String) = new ErrorParser(message)

  /**
   * A parser that consumes no data and returns `value`.
   */
  def success[T](value: T) = new ReturnParser(value)

  /**
   * A parser that consumes no data and returns Unit.
   */
  val unit = success(())

  /**
   * A parser that consumes no data and succeeds or fails based on whether or not `opt` is defined.
   */
  def liftOpt[T](opt: Option[T]): Parser[T] = opt match {
    case Some(r) => success(r)
    case None    => fail("Parse failed.")
  }


  /* Behavior Transformation */

  /**
   * Parse `p` zero or one time.
   */
  def opt[T](p: Parser[T]): Parser[Option[T]] = p map { Some(_) } or success(None)

  /**
   * Convert unrecoverable Errors into Fails, by saving a marker of
   * the current ChannelBuffer state, and only advancing if `p`
   * succeeds. If `p` Errors, reset the ChannelBuffer, and Fail
   * instead.
   */
  def attempt[T](p: Parser[T]) = new BacktrackingParser(p)


  /* Repetition */

  /**
   * Parse `p` zero or more times.
   */
  def rep[T](p: Parser[T]): Parser[Seq[T]] = new RepeatParser(p)

  /**
   * Parse `p` one time, followed by `q` zero or more times.
   */
  def rep1[T](p: Parser[T], q: Parser[T]): Parser[Seq[T]] = {
    val getRest = rep(q)

    for (head <- p; tail <- getRest) yield (head +: tail)
  }

  /**
   * Parse `p` one or more times.
   */
  def rep1[T](p: Parser[T]): Parser[Seq[T]] = {
    rep1(p, p)
  }

  /**
   * Parse `p` one or more times, separated by `sep`.
   */
  def rep1sep[T](p: Parser[T], sep: Parser[Any]): Parser[Seq[T]] = {
    rep1(p, sep then p)
  }

  /**
   * Parse `p` zero or more times, separated by `sep`.
   */
  def repsep[T](p: Parser[T], sep: Parser[Any]): Parser[Seq[T]] = {
    rep1sep(p, sep) or success[Seq[T]](Nil)
  }

  /**
   * Parse `p` a total of `count` times.
   */
  def repN[T](count: Int, p: Parser[T]): Parser[Seq[T]] = {
    new RepeatTimesParser(p, count)
  }


  /* Matching Parsers */

  /**
   * Succeed parsing if the input buffer matches `m`.
   *
   * Returns a ChannelBuffer of the bytes matched by `m`.
   */
  def accept(m: Matcher) = new MatchParser(m) flatMap { readBytes(_) }

  implicit def acceptString(choice: String) = {
    val bytes = choice.getBytes("US-ASCII")
    new MatchParser(new BytesMatcher(bytes)) then skipBytes(bytes.size)
  }

  /**
   * Succeed parsing if the input buffer starts with `choice`.
   *
   * Returns a ChannelBuffer of the bytes matched by `choice`.
   */
  def accept(choice: String): Parser[ChannelBuffer] = {
    accept(new BytesMatcher(choice))
    // val m = new MatchParser(new BytesMatcher(choice))
    // m then skipBytes(choice.size) then success(choice)
  }

  /**
   * Succeed parsing if the input buffer starts with any of the alternatives.
   *
   * Returns a ChannelBuffer of the bytes matched.
   */
  def accept(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    accept(AlternateMatcher(first +: second +: rest))
  }

  /**
   * Succeed parsing if the input buffer matches `m`.
   *
   * Returns the length of the match, consuming no data.
   */
  def guard(m: Matcher) = new MatchParser(m)

  /**
   * Succeed parsing if the input buffer starts with `choice`.
   *
   * Returns the length of `choice`, consuming no data.
   */
  def guard(choice: String): Parser[Int] = {
    guard(new BytesMatcher(choice))
  }

  /**
   * Succeed parsing if the input buffer starts with any of the alternatives.
   *
   * Returns the length of the match, consuming no data.
   */
  def guard(first: String, second: String, rest: String*): Parser[Int] = {
    guard(AlternateMatcher(first +: second +: rest))
  }

  /**
   * Succeed in parsing if the input buffer does not match `m`.
   *
   * Returns Unit, consuming no data.
   */
  def not(m: Matcher) = guard(m.negate) then unit

  /**
   * Succeed in parsing if the input buffer does not start with `choice`.
   *
   * Returns Unit, consuming no data.
   */
  def not(choice: String): Parser[Unit] = {
    not(new BytesMatcher(choice))
  }

  /**
   * Succeed in parsing if the input buffer does not start with any of the alternatives.
   *
   * Returns Unit, consuming no data.
   */
  def not(first: String, second: String, rest: String*): Parser[Unit] = {
    not(AlternateMatcher(first +: second +: rest))
  }

  def choice[T](choices: (String, Parser[T])*): Parser[T] = {
    val (m, p)           = choices.head
    val first: Parser[T] = accept(m) then p
    val rest             = choices.tail

    if (rest.isEmpty) first else first or choice(rest: _*)
  }


  /* Delimiter Parsers */

  /**
   * Reads bytes up to and including those matched by `m`.
   *
   * Returns all bytes before the match.
   */
  def readTo(m: Matcher) = new ConsumingDelimiterParser(m)

  /**
   * Reads bytes up to and including `choice`.
   *
   * Returns all bytes before `choice`.
   */
  def readTo(choice: String): Parser[ChannelBuffer] = {
    readTo(new BytesMatcher(choice))
  }

  /**
   * Reads bytes up to and including the first occurrence of any of the alternatives.
   *
   * Returns all bytes before the match.
   */
  def readTo(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    readTo(AlternateMatcher(first +: second +: rest))
  }

  /**
   * Returns the number of bytes before `s`. Consumes no data.
   */
  def bytesBefore(s: String) = {
    new DelimiterFinderParser(new BytesMatcher(s.getBytes("US-ASCII")))
  }

  /**
   * Reads bytes up to but excluding those matched by `m`.
   */
  def readUntil(m: Matcher) = new DelimiterParser(m)

  /**
   * Reads bytes up to but excluding `choice'.
   */
  def readUntil(choice: String): Parser[ChannelBuffer] = {
    readUntil(new BytesMatcher(choice))
  }

  /**
   * Reads bytes up to but excluding the first occurrence of any of the alternatives.
   */
  def readUntil(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    readUntil(AlternateMatcher(first +: second +: rest))
  }

  /**
   * Reads all bytes matching `m`.
   */
  def readWhile(m: Matcher) = readUntil(new NotMatcher(m))

  /**
   * Reads all bytes matching `choice`.
   */
  def readWhile(choice: String): Parser[ChannelBuffer] = {
    readWhile(new BytesMatcher(choice))
  }

  /**
   * Reads all bytes while matching one of the alternatives.
   */
  def readWhile(first: String, second: String, rest: String*): Parser[ChannelBuffer] = {
    readWhile(AlternateMatcher(first +: second +: rest))
  }


  /* Basic Bytes Parsers */

  /**
   * Pass the underlying input buffer to `f`, allowign for arbitrary
   * manipulation of the underlying buffer. The current readerIndex of
   * the buffer is saved, and is reset if IndexOutOfBoundsException is
   * thrown. In this sense, it behaves somewhat like Netty's
   * ReplayingCodec. `f` can potentially be called multiple times if
   * there is insufficient data to satisfy `f`.
   */
  def withRawBuffer[T](f: ChannelBuffer => T) = new RepeatingRawBufferParser(f)

  /**
   * Read `count` bytes. If `count` is large enough, this will create
   * a new dynamic ChannelBuffer, and store the intermediate chunks.
   * Otherwise, if `count` is small, this calls `readSlice`.
   */
  def readBytes(count: Int) = new BytesParser(count)

  /**
   * Skip `count` bytes.
   */
  def skipBytes(count: Int) = new SkipBytesParser(count)


  /* Primitive Types Parsers */

  protected def unsafeWithRawBuffer[T](f: ChannelBuffer => T) = new RawBufferParser(f)

  /**
   * Read a signed byte.
   */
  val readByte = unsafeWithRawBuffer { _.readByte }

  /**
   * Read a signed 16-bit short integer.
   */
  val readShort = unsafeWithRawBuffer { _.readShort }

  /**
   * Read a signed 24-bit medium integer.
   */
  val readMedium = unsafeWithRawBuffer { _.readMedium }

  /**
   * Read a signed 32-bit integer.
   */
  val readInt = unsafeWithRawBuffer { _.readInt }

  /**
   * Read a signed 64-bit integer.
   */
  val readLong = unsafeWithRawBuffer { _.readLong }

  /**
   * Read an unsigned byte.
   */
  val readUnsignedByte = unsafeWithRawBuffer { _.readUnsignedByte }

  /**
   * Read an unsigned 16-bit short integer.
   */
  val readUnsignedShort = unsafeWithRawBuffer { _.readUnsignedShort }

  /**
   * Read an unsigned 24-bit medium integer.
   */
  val readUnsignedMedium = unsafeWithRawBuffer { _.readUnsignedMedium }

  /**
   * Read an unsigned 32-bit integer.
   */
  val readUnsignedInt = unsafeWithRawBuffer { _.readUnsignedInt }

  /**
   * Read a 2-byte UTF-16 character.
   */
  val readChar = unsafeWithRawBuffer { _.readChar }

  /**
   * Read a 64-bit floating point number.
   */
  val readDouble = unsafeWithRawBuffer { _.readDouble }

  /**
   * Read a 32-bit floating point number.
   */
  val readFloat = unsafeWithRawBuffer { _.readFloat }
}
