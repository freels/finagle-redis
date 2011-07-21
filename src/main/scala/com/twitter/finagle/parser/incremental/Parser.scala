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

  def then[T](rhs: Parser[T]) = new ThenParser(this, rhs)

  def then[T](rv: T) = new ThenParser(this, success(rv))

  def through[T](rhs: Parser[T]) = this into { rhs then success(_) }

  def and[T, C <: ChainableTuple](rhs: Parser[T])(implicit chn: Out => C): Parser[C#Next[T]] = {
    for (tup <- this; next <- rhs) yield chn(tup).append(next)
  }

  def or[O >: Out](rhs: Parser[O]) = new OrParser(this, rhs)

  def into[T](f: Out => Parser[T]): Parser[T] = new IntoParser(this, f)


  // Satify monadic api

  def flatMap[T](f: Out => Parser[T]) = this into f

  def map[T](f: Out => T): Parser[T] = this into { o => success(f(o)) }


  // yay operators...this may be a bad idea.

  def * = rep(this)

  def + = rep1(this)

  def ? = opt(this)

  def <<[T](rhs: Parser[T]) = this through rhs

  def >>[T](rhs: Parser[T]) = this then rhs

  def >>=[T](f: Out => Parser[T]) = this into f

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

class ThenParser[+Out](parser: Parser[_], tail: Parser[Out]) extends CompoundParser[Out] {
  override def decodeStep(buffer: ChannelBuffer) = {
    parser.decode(buffer) then tail
  }

  override def then[T](other: Parser[T]) = {
    new ThenParser(parser, tail then other)
  }

  override def into[T](f: Out => Parser[T]): Parser[T] = {
    new ThenParser(parser, tail into f)
  }
}

class IntoParser[T, +Out](parser: Parser[T], f: T => Parser[Out])
extends CompoundParser[Out] {
  override def decodeStep(buffer: ChannelBuffer) = {
    parser.decode(buffer) into f
  }
}

class OrParser[+Out](choice: Parser[Out], tail: Parser[Out], committed: Boolean)
extends CompoundParser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  override def decodeStep(buffer: ChannelBuffer) = {
    val start  = buffer.readerIndex
    val result = choice.decode(buffer)

    result.or(tail, committed || buffer.readerIndex > start)
  }

  override def or[O >: Out](other: Parser[O]) = {
    new OrParser(choice, tail or other)
  }
}

class NotParser(parser: Parser[_]) extends Parser[Unit] {

  def decode(buffer: ChannelBuffer) = {
    val start = buffer.readerIndex
    val result = parser.decode(buffer)

    result.negate(buffer.readerIndex > start)
  }

  def fail() = Fail("Expected "+ parser +" to fail.")
  def error() = Error("Expected "+ parser +" to fail, but already consumed data.")
}
