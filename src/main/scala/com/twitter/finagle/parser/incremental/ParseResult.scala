package com.twitter.finagle.parser.incremental

// states: continue (wait), return, fail (recoverable), error

sealed trait ParseResult[+Out]

abstract class ParseException extends Throwable {
  def message: String

  override def fillInStackTrace(): Throwable = null

  def realFillInStackTrace() = super.fillInStackTrace()

  override def getMessage() = message
}

case class Return[@specialized +Out](ret: Out) extends ParseResult[Out]

case class Continue[+Out](next: Parser[Out]) extends ParseException with ParseResult[Out] {
  def message = "Insufficient data."
}

case class Fail(messageString: () => String) extends ParseException with ParseResult[Nothing] {
  def message = messageString()
}

case class Error(messageString: () => String) extends ParseException with ParseResult[Nothing] {
  def message = messageString()
}
