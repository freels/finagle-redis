package com.twitter.finagle.parser.util


object Matchers {
  val CRLF       = new DelimiterMatcher("\r\n")
  val WhiteSpace = AlternateMatcher(Seq(" ", "\t", "\r\n", "\n"))
}
