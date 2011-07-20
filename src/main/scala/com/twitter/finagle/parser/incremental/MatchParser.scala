package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.Matcher


class MatchParser(matcher: Matcher) extends Parser[Int] {
  def decode(buffer: ChannelBuffer): ParseResult[Int] = {
    matcher.bytesMatching(buffer, buffer.readerIndex) match {
      case -2        => Fail("Match failed.")
      case -1        => Continue(this)
      case matchSize => Return(matchSize)
    }
  }
}
