package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
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
  def decodeRaw(buffer: ChannelBuffer) = f(buffer)
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



final class RepeatParser[+Out](
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
      case Continue(rest) => throw Continue(new RepeatParser(parser, total, result, i, rest))
        case e => println(e); throw e
    }

    result.toSeq.asInstanceOf[Seq[Out]]
  }
}

final class OrParser[+Out](choice: Parser[Out], tail: Parser[Out], committed: Boolean)
extends Parser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  def decodeRaw(buffer: ChannelBuffer): Out = {
    sys.error("Not implemented")
    // val start  = buffer.readerIndex
    // choice.decodeWithState(state, buffer)
    // val newCommitted = committed || buffer.readerIndex > start

    // if (state.isCont) {
    //   state.cont(new OrParser(state.nextParser, tail, newCommitted))
    // } else if (state.isFail) {
    //   if (newCommitted) {
    //     state.error(state.errorMessage)
    //   } else {
    //     tail.decodeWithState(state, buffer)
    //   }
    // }
  }

  override def or[O >: Out](other: Parser[O]): Parser[O] = {
    new OrParser(choice, tail or other)
  }
}

final class NotParser(parser: Parser[_]) extends Parser[Unit] {

  def decodeRaw(buffer: ChannelBuffer) = {
    sys.error("Not implemented")
    // val start     = buffer.readerIndex
    // parser.decodeWithState(state, buffer)
    // val committed = buffer.readerIndex > start

    // if (state.isCont) {
    //   if (committed) {
    //     state.error("Expected "+ parser +" to fail, but already consumed data.")
    //   } else {
    //     state.cont(new NotParser(state.nextParser))
    //   }
    // } else if (state.isRet) {
    //   if (committed) {
    //     state.error("Expected "+ parser +" to fail, but already consumed data.")
    //   } else {
    //     state.fail("Expected "+ parser +" to fail.")
    //   }
    // } else if (state.isFail) {
    //   if (committed) {
    //     state.error(state.errorMessage)
    //   } else {
    //     state.ret(())
    //   }
    // }
  }
}
