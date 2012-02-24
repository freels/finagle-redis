package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.Matcher


class MatchParser(matcher: Matcher) extends Parser[Int] {
  def decodeWithState(state: ParseState, buffer: ChannelBuffer) {
    matcher.bytesMatching(buffer, buffer.readerIndex) match {
      case -2        => state.fail("Match failed.")
      case -1        => state.cont(this)
      case matchSize => state.ret(matchSize)
    }
  }
}
