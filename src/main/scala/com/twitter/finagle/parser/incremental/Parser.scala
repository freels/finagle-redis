package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.ChainableTuple

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

  def then[T](rhs: Parser[T]): Parser[T] = new ThenParser(this, rhs)

  def then[T](rv: T): Parser[T] = new ThenParser(this, success(rv))

  def through[T](rhs: Parser[T]): Parser[Out] = new ThroughParser(this, rhs)

  def and[T, C <: ChainableTuple](rhs: Parser[T])(implicit chn: Out => C): Parser[C#Next[T]] = {
    for (tup <- this; next <- rhs) yield chn(tup).append(next)
  }

  def or[O >: Out](rhs: Parser[O]): Parser[O] = new OrParser(this, rhs)

  def flatMap[T](f: Out => Parser[T]): Parser[T] = new FlatMapParser(this, f)

  def map[T](f: Out => T): Parser[T] = new MapParser(this, f)


  // yay operators...this may be a bad idea.

  def * = rep(this)

  def + = rep1(this)

  def ? = opt(this)

  def <<[T](rhs: Parser[T]) = this through rhs

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

final class FailParser(message: String) extends Parser[Nothing] {
  def decodeRaw(buffer: ChannelBuffer) = throw Fail(message)
}

final class ErrorParser(message: String) extends Parser[Nothing] {
  def decodeRaw(buffer: ChannelBuffer) = throw Error(message)
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

final class FlatMapParser[T, +Out](parser: Parser[T], f: T => Parser[Out]) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next = try f(parser.decodeRaw(buffer)) catch {
      case Continue(rest) =>
        throw Continue(new FlatMapParser(rest.asInstanceOf[Parser[T]], f))
    }

    next.decodeRaw(buffer)
  }
}

final class MapParser[T, +Out](parser: Parser[T], f: T => Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val rv = try parser.decodeRaw(buffer) catch {
      case Continue(rest) =>
        throw Continue(new MapParser(rest.asInstanceOf[Parser[T]], f))
    }

    f(rv)
  }
}


final class ThenParser[+Out](parser: Parser[_], next: Parser[Out]) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    try parser.decodeRaw(buffer) catch {
      case Continue(rest) => throw Continue(new ThenParser(rest, next))
    }

    next.decodeRaw(buffer)
  }

  override def then[T](other: Parser[T]): Parser[T] = {
    new ThenParser(parser, next then other)
  }
}

final class ThroughParser[+Out](parser: Parser[Out], next: Parser[_]) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val rv = try parser.decodeRaw(buffer) catch {
      case Continue(rest) => throw Continue(new ThroughParser(rest, next))
    }

    try next.decodeRaw(buffer) catch {
      case Continue(rest) => throw Continue(new ConstParser(parser, rv))
    }

    rv
  }
}

final class ConstParser[+Out](parser: Parser[Out], value: Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    try parser.decodeRaw(buffer) catch {
      case Continue(rest) => throw Continue(new ConstParser(parser, value))
    }

    value
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
        case e => println(e); throw e
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

final class OrParser[+Out](choice: Parser[Out], next: Parser[Out], committed: Boolean)
extends Parser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  def decodeRaw(buffer: ChannelBuffer): Out = {
    val start = buffer.readerIndex

    try choice.decodeRaw(buffer) catch {
      case f: Fail =>
        if (committed || buffer.readerIndex > start) throw Error(f.message)
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
          throw Error("Expected "+ parser +" to fail, but already consumed data.")
        } else {
          throw Continue(new NotParser(rest))
        }
      case f: Fail => ()
    }

    if (succeeded) {
      if (buffer.readerIndex > start) {
        throw Error("Expected "+ parser +" to fail, but already consumed data.")
      } else {
        throw Fail("Expected "+ parser +" to fail.")
      }
    }
  }
}
