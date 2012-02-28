package com.twitter.finagle.parser.incremental

// states: continue (wait), return, fail (recoverable), error

sealed trait ParseResult[+Out]

abstract class BlankThrowable extends Throwable {
  override def fillInStackTrace(): Throwable = null
}

case class Continue[+Out](next: Parser[Out]) extends BlankThrowable with ParseResult[Out]

case class Return[@specialized +Out](ret: Out) extends ParseResult[Out]

case class Fail(messageString: () => String) extends BlankThrowable with ParseResult[Nothing] {
  override def getMessage() = messageString()
  def message = messageString()
}

case class Error(messageString: () => String) extends BlankThrowable with ParseResult[Nothing] {
  override def getMessage() = messageString()
  def message = getMessage()
}
