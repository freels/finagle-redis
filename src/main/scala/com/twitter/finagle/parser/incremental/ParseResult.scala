package com.twitter.finagle.parser.incremental

// states: continue (wait), return, fail (recoverable), error

sealed abstract class ParseResult[+Out]

case class Continue[+Out](next: Parser[Out]) extends ParseResult[Out]

case class Return[+Out](ret: Out) extends ParseResult[Out]

case class Fail(message: String) extends ParseResult[Nothing]

case class Error(message: String) extends ParseResult[Nothing]
