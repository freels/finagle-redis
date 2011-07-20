package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.ParseException


// states: continue (wait), return, error

sealed abstract class ParseResult[+Out] {
  private[incremental] def append[T](p: Parser[T]): Parser[T]
  private[incremental] def into[O >: Out, T](f: O => Parser[T]): Parser[T]
  private[incremental] def or[O >: Out](p: Parser[O], committed: Boolean): Parser[O]

  private[incremental] def negate(committed: Boolean): ParseResult[Unit]

  protected def end[T](r: ParseResult[T]) = new LiftParser(r)
}

case class Continue[+Out](next: Parser[Out]) extends ParseResult[Out] {
  private[incremental] def append[T](p: Parser[T]) = end(Continue(next append p))
  private[incremental] def into[O >: Out, T](f: O => Parser[T]) = end(Continue(next into f))
  private[incremental] def or[O >: Out](p: Parser[O], committed: Boolean) = end(Continue(
    new OrParser(next, p, committed)
  ))

  private[incremental] def negate(committed: Boolean) = if (committed) {
    Error("Expected fail, but already consumed data.")
  } else {
    Continue(new NotParser(next))
  }
}

case class Return[+Out](ret: Out) extends ParseResult[Out] {
  private[incremental] def append[T](p: Parser[T]) = p
  private[incremental] def into[O >: Out, T](f: O => Parser[T]) = f(ret)
  private[incremental] def or[O >: Out](p: Parser[O], committed: Boolean) = end(this)

  private[incremental] def negate(committed: Boolean) = if (committed) {
    Error("Expected parse fail, but already consumed data.")
  } else {
    Fail("Expected parse fail.")
  }
}

case class Fail(message: String) extends ParseResult[Nothing] {
  private[incremental] def append[T](p: Parser[T]) = end(this)
  private[incremental] def into[O >: Nothing, T](f: O => Parser[T]) = end(this)
  private[incremental] def or[O >: Nothing](p: Parser[O], committed: Boolean) = {
    if (committed) end(Error(message)) else p
  }

  private[incremental] def negate(committed: Boolean) = {
    if (committed) Error(message) else Return(())
  }

  def throwException() = throw new ParseException(message)
}
case class Error(message: String) extends ParseResult[Nothing] {
  private[incremental] def append[T](p: Parser[T]) = end(this)
  private[incremental] def into[O >: Nothing, T](f: O => Parser[T]) = end(this)
  private[incremental] def or[O >: Nothing](p: Parser[O], committed: Boolean) = end(this)

  private[incremental] def negate(committed: Boolean) = this

  def throwException() = throw new ParseException(message)
}


abstract class Parser[+Out] {
  import Parsers._

  def decode(buffer: ChannelBuffer): ParseResult[Out]

  // overridden in link parsers

  def hasNext = false

  def decodeStep(buffer: ChannelBuffer): Parser[Out] = {
    throw new NoSuchElementException("End of a parse chain.")
  }

  // basic composition

  def append[T](rhs: Parser[T]) = new AppendParser(this, rhs)

  def and[T](rhs: Parser[T]): Parser[Out ~ T] = {
    for (l <- this; r <- rhs) yield new ~(l, r)
  }

  def or[O >: Out](rhs: Parser[O]) = new OrParser(this, rhs)

  def into[T](f: Out => Parser[T]): Parser[T] = new IntoParser(this, f)


  // Satify monadic api

  def flatMap[T](f: Out => Parser[T]) = this into f

  def flatMap[T](p: Parser[T]) = this and p

  def map[T](f: Out => T): Parser[T] = this into { o => success(f(o)) }


  // yay operators...this may be a bad idea.

  def * = rep(this)

  def + = rep1(this)

  def <~[T](rhs: Parser[T]) = this into { x => rhs append success(x) }

  def >>[T](f: Out => Parser[T]) = this into f

  def ? = this map { Some(_) } or success(None)

  def ^?[T](f: PartialFunction[Out, T], err: Out => String) = this into { o =>
    if (f.isDefinedAt(o)) success(f(o)) else fail(err(o))
  }

  def ^?[T](f: PartialFunction[Out, T]): Parser[T] = {
    this ^? (f, _ => "Function not defined for value.")
  }

  def ^^[T](f: Out => T) = this map f

  def ^^^[T](r: T) = this append success(r)

  def |[T](rhs: Parser[T]) = this or rhs

  def ~[T](rhs: Parser[T]) = this and rhs

  def ~>[T](rhs: Parser[T]) = this append rhs
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
  override def decodeStep(buffer: ChannelBuffer) = {
    parser.decode(buffer) append tail
  }

  override def append[T](other: Parser[T]) = {
    new AppendParser(parser, tail append other)
  }

  override def into[T](f: Out => Parser[T]): Parser[T] = {
    new AppendParser(parser, tail into f)
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
