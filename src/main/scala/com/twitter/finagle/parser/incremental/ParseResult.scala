package com.twitter.finagle.parser.incremental

import com.twitter.finagle.ParseException


// states: continue (wait), return, error

sealed abstract class ParseResult[+Out] {
  private[incremental] def then[T](p: Parser[T]): Parser[T]
  private[incremental] def flatMap[O >: Out, T](f: O => Parser[T]): Parser[T]
  private[incremental] def or[O >: Out](p: Parser[O], committed: Boolean): Parser[O]

  private[incremental] def negate(committed: Boolean): ParseResult[Unit]

  protected def end[T](r: ParseResult[T]) = new LiftParser(r)
}

case class Continue[+Out](next: Parser[Out]) extends ParseResult[Out] {
  private[incremental] def then[T](p: Parser[T]) = end(Continue(next then p))
  private[incremental] def flatMap[O >: Out, T](f: O => Parser[T]) = end(Continue(next flatMap f))
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
  private[incremental] def then[T](p: Parser[T]) = p
  private[incremental] def flatMap[O >: Out, T](f: O => Parser[T]) = f(ret)
  private[incremental] def or[O >: Out](p: Parser[O], committed: Boolean) = end(this)

  private[incremental] def negate(committed: Boolean) = if (committed) {
    Error("Expected parse fail, but already consumed data.")
  } else {
    Fail("Expected parse fail.")
  }
}

case class Fail(message: String) extends ParseResult[Nothing] {
  private[incremental] def then[T](p: Parser[T]) = end(this)
  private[incremental] def flatMap[O >: Nothing, T](f: O => Parser[T]) = end(this)
  private[incremental] def or[O >: Nothing](p: Parser[O], committed: Boolean) = {
    if (committed) end(Error(message)) else p
  }

  private[incremental] def negate(committed: Boolean) = {
    if (committed) Error(message) else Return(())
  }

  def throwException() = throw new ParseException(message)
}

case class Error(message: String) extends ParseResult[Nothing] {
  private[incremental] def then[T](p: Parser[T]) = end(this)
  private[incremental] def flatMap[O >: Nothing, T](f: O => Parser[T]) = end(this)
  private[incremental] def or[O >: Nothing](p: Parser[O], committed: Boolean) = end(this)

  private[incremental] def negate(committed: Boolean) = this

  def throwException() = throw new ParseException(message)
}
