package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.Matcher


class MatchParser(matcher: Matcher) extends Parser[Int] {
  def decodeRaw(buffer: ChannelBuffer) = {
    matcher.bytesMatching(buffer, buffer.readerIndex) match {
      case -2        => sys.error("Match failed.")
      case -1        => sys.error("Match inconclusive.")
      case matchSize => matchSize
    }
  }
}
