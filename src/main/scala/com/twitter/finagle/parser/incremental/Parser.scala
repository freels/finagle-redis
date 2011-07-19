package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.ParseException


// states: continue (wait), return, error

sealed trait ParseResult[+Out]

case class Continue[+Out](next: Parser[Out]) extends ParseResult[Out]
case class Return[+Out](ret: Out) extends ParseResult[Out]
case class Fail(ex: ParseException) extends ParseResult[Nothing]
case class Error(ex: ParseException) extends ParseResult[Nothing]


abstract class Parser[+Out] {
  import Parsers._

  def decode(buffer: ChannelBuffer): ParseResult[Out]

  // overridden in link parsers

  def hasNext = false

  def decodeStep(buffer: ChannelBuffer): Parser[Out] = {
    throw new NoSuchElementException("End of a parse chain.")
  }

  // basic composition api

  def append[T](rhs: Parser[T]) = new AppendParser(this, rhs)

  def and[T](rhs: Parser[T]): Parser[(Out, T)] = {
    for (l <- this; r <- rhs) yield Pair(l, r)
  }

  def or[O >: Out](rhs: Parser[O]) = new OrParser(this, rhs)

  def into[T](f: Out => Parser[T]): Parser[T] = new IntoParser(this, f)


  // Satify monadic api

  def flatMap[T](f: Out => Parser[T]) = this into f

  def flatMap[T](p: Parser[T]) = this and p

  def map[T](f: Out => T): Parser[T] = this into { out => success(f(out)) }
}


class LiftParser[+Out](r: ParseResult[Out]) extends Parser[Out] {
  def decode(buffer: ChannelBuffer) = r
}


sealed abstract class CompoundParser[+Out] extends Parser[Out] {
  override def hasNext = true

  def decode(buffer: ChannelBuffer): ParseResult[Out] = {
    var curr: Parser[Out] = this

    @tailrec
    def step(p: Parser[Out]): ParseResult[Out] = {
      if (p.hasNext) step(p.decodeStep(buffer)) else p.decode(buffer)
    }

    step(this)
  }

  protected[this] def end(r: ParseResult[Out]) = new LiftParser(r)
}

class AppendParser[+Out](parser: Parser[_], tail: Parser[Out]) extends CompoundParser[Out] {
  override def decodeStep(buffer: ChannelBuffer) = parser.decode(buffer) match {
    case Return(_)      => tail
    case Continue(next) => if (next eq parser) {
      end(Continue(this))
    } else {
      end(Continue(new AppendParser(next, tail)))
    }
    case e: Fail  => end(e)
    case e: Error => end(e)
  }

  override def append[T](other: Parser[T]) = {
    new AppendParser(parser, tail append other)
  }
}

class IntoParser[T, +Out](parser: Parser[T], f: T => Parser[Out])
extends CompoundParser[Out] {
  override def decodeStep(buffer: ChannelBuffer) = parser.decode(buffer) match {
    case Return(r)      => f(r)
    case Continue(next) => if (next eq parser) {
      end(Continue(this))
    } else {
      end(Continue(new IntoParser(next, f)))
    }
    case e: Fail  => end(e)
    case e: Error => end(e)
  }
}

class OrParser[+Out](choice: Parser[Out], tail: Parser[Out], committed: Boolean)
extends CompoundParser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  override def decodeStep(buffer: ChannelBuffer) = {
    val start = buffer.readerIndex

    choice.decode(buffer) match {
      case r: Return[Out] => end(r)
      case e: Fail => if (committed || buffer.readerIndex != start) {
        end(Error(e.ex))
      } else {
        tail
      }
      case Continue(next) => {
        if ((next eq choice) && buffer.readerIndex == start) {
          end(Continue(this))
        } else {
          end(Continue(new OrParser(next, tail, committed || buffer.readerIndex != start)))
        }
      }
      case e: Error => end(e)
    }
  }

  override def or[O >: Out](other: Parser[O]) = {
    new OrParser(choice, tail or other)
  }
}

class NotParser(parser: Parser[_]) extends Parser[Unit] {

  def decode(buffer: ChannelBuffer) = {
    val start = buffer.readerIndex

    parser.decode(buffer) match {
      case r: Return[_] => {
        if (buffer.readerIndex != start) {
          error()
        } else {
          fail()
        }
      }
      case e: Fail => {
        if (buffer.readerIndex != start) {
          Error(e.ex)
        } else {
          Return(())
        }
      }
      case Continue(next) => {
        if (buffer.readerIndex != start) {
          error()
        } else if (next == parser) {
          Continue(this)
        } else {
          Continue(new NotParser(next))
        }
      }
      case e: Error => e
    }
  }

  def fail() = Fail(new ParseException("Expected "+ parser +" to fail."))
  def error() = Error(new ParseException(
    "Expected "+ parser +" to fail, but already consumed data."
  ))
}
