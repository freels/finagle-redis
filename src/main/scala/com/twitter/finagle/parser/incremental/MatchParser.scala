package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util._


abstract class AbstractMatchParser[+Out](matcher: Matcher) extends Parser[Out] {

  protected def matchSucceeded(buffer: ChannelBuffer, matchSize: Int): ParseResult[Out]

  def decode(buffer: ChannelBuffer): ParseResult[Out] = {
    matcher.bytesMatching(buffer, buffer.readerIndex) match {
      case -2        => matchFailed
      case -1        => matchInconclusive
      case matchSize => matchSucceeded(buffer, matchSize)
    }
  }

  protected def matchFailed: ParseResult[Out] = {
    new Fail("Match failed.")
  }

  protected def matchInconclusive: ParseResult[Out] = {
    Continue(this)
  }
}

class MatchParser(matcher: Matcher)
extends AbstractMatchParser[ChannelBuffer](matcher) {
  def this(bytes: Array[Byte]) = this(new DelimiterMatcher(bytes))
  def this(string: String) = this(new DelimiterMatcher(string))

  def matchSucceeded(buffer: ChannelBuffer, matchSize: Int) = {
    Return(buffer.slice(buffer.readerIndex, matchSize))
  }
}

class ConsumingMatchParser(matcher: Matcher)
extends AbstractMatchParser[ChannelBuffer](matcher) {

  def this(bytes: Array[Byte]) = this(new DelimiterMatcher(bytes))

  def this(string: String) = this(new DelimiterMatcher(string))

  def matchSucceeded(buffer: ChannelBuffer, matchSize: Int) = {
    Return(buffer.readSlice(matchSize))
  }
}
