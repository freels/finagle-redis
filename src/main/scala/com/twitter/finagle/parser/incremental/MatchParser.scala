package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.Matcher


class MatchParser(matcher: Matcher) extends Parser[Int] {
  def decodeRaw(buffer: ChannelBuffer) = {
    val size = matcher.bytesMatching(buffer, buffer.readerIndex)

    if (size < 0) {
      if (size == -1) {
        sys.error("Match inconclusive.")
      } else {
        sys.error("Match failed.")
      }
    } else {
      size
    }
  }
}
