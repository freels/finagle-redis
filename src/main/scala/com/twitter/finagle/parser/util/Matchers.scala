package com.twitter.finagle.parser.util


object Matchers {
  val CRLF       = AlternateMatcher(Seq("\r\n", "\n"))
  val WhiteSpace = AlternateMatcher(Seq(" ", "\t", "\r\n", "\n"))
}
