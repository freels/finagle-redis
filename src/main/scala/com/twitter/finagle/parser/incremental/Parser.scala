package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
import scala.collection.mutable.{ListBuffer, WrappedArray}
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.ChainableTuple

/**
 * The base class for all incremental ChannelBuffer parsers.
 *
 * Incremental parsers are designed to process Netty ChannelBuffers that may contain only partial messages. The API is inspired by existing parser combinator libraries like Haskell's Parsec and Scala's built in parser combinators. As an example, here is a full parser of Redis's reply protocol:
 *
 * {{{
 *
 * }}}
 *
 *
 */
abstract class Parser[+Out] {
  import Parsers._

  def decode(buffer: ChannelBuffer): ParseResult[Out] = {
    try Return(decodeRaw(buffer)) catch {
      case c: Continue[_] => c.asInstanceOf[Continue[Out]]
      case f: Fail => f
      case e: Error => e
    }
  }

  def decodeRaw(buffer: ChannelBuffer): Out


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
