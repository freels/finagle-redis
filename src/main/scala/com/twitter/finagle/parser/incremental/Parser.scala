package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.ChainableTuple


abstract class Parser[+Out] {
  import Parsers._

  def decode(buffer: ChannelBuffer): ParseResult[Out]

  // overridden in compound parsers

  def hasNext = false

  def decodeStep(buffer: ChannelBuffer): Parser[Out] = {
    throw new NoSuchElementException("End of a parse chain.")
  }

  // basic composition

  def then[T](rhs: Parser[T]) = this flatMap { _ => rhs }

  def then[T](rv: T) = this flatMap { _ => success(rv) }

  def through[T](rhs: Parser[T]) = this flatMap { rhs then success(_) }

  def and[T, C <: ChainableTuple](rhs: Parser[T])(implicit chn: Out => C): Parser[C#Next[T]] = {
    for (tup <- this; next <- rhs) yield chn(tup).append(next)
  }

  def or[O >: Out](rhs: Parser[O]) = new OrParser(this, rhs)

  def flatMap[T](f: Out => Parser[T]): Parser[T] = new FlatMapParser(this, f)

  def map[T](f: Out => T): Parser[T] = this flatMap { o => success(f(o)) }


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


class LiftParser[+Out](r: ParseResult[Out]) extends Parser[Out] {
  def decode(buffer: ChannelBuffer) = r
}


abstract class CompoundParser[+Out] extends Parser[Out] {
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

class FlatMapParser[T, +Out](parser: Parser[T], f: T => Parser[Out])
extends CompoundParser[Out] {
  override def decodeStep(buffer: ChannelBuffer) = {
    //parser.decode(buffer) flatMap f
    parser.decode(buffer) match {
      case c: Continue[T] => end(Continue(c.next flatMap f))
      case r: Return[T]   => f(r.ret)
      case f: Fail        => end(f)
      case e: Error       => end(e)
    }
  }
}

class OrParser[+Out](choice: Parser[Out], tail: Parser[Out], committed: Boolean)
extends CompoundParser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  override def decodeStep(buffer: ChannelBuffer) = {
    val start  = buffer.readerIndex
    val result = choice.decode(buffer)
    val newCommitted = committed || buffer.readerIndex > start

    //result.or(tail, newCommitted)
    result match {
      case c: Continue[Out] => end(Continue(new OrParser(c.next, tail, newCommitted)))
      case r: Return[Out] => end(r)
      case f: Fail        => if (newCommitted) end(Error(f.message)) else tail
      case e: Error       => end(e)
    }
  }

  override def or[O >: Out](other: Parser[O]) = {
    new OrParser(choice, tail or other)
  }
}

class NotParser(parser: Parser[_]) extends Parser[Unit] {

  def decode(buffer: ChannelBuffer) = {
    val start     = buffer.readerIndex
    val result    = parser.decode(buffer)
    val committed = buffer.readerIndex > start

    //result.negate(committed)
    result match {
      case c: Continue[_] => if (committed) {
        Error("Expected fail, but already consumed data.")
      } else {
        Continue(new NotParser(c.next))
      }
      case r: Return[_] => if (committed) {
        Error("Expected parse fail, but already consumed data.")
      } else {
        Fail("Expected parse fail.")
      }
      case f: Fail => if (committed) Error(f.message) else Return(())
      case e: Error => e
    }
  }

  def fail() = Fail("Expected "+ parser +" to fail.")
  def error() = Error("Expected "+ parser +" to fail, but already consumed data.")
}
