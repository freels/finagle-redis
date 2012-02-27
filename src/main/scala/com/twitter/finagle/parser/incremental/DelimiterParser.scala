package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util._

class DelimiterFinderParser(matcher: Matcher) extends Parser[Int] {
  def this(bytes: Array[Byte]) = this(new DelimiterMatcher(bytes))

  def this(string: String) = this(new DelimiterMatcher(string))

  def decodeRaw(buffer: ChannelBuffer) = {
    buffer.bytesBefore(matcher)
  }
}

class DelimiterParser(matcher: Matcher) extends Parser[ChannelBuffer] {

  def this(bytes: Array[Byte]) = this(new DelimiterMatcher(bytes))

  def this(string: String) = this(new DelimiterMatcher(string))

  def decodeRaw(buffer: ChannelBuffer) = {
    val frameLength = buffer.bytesBefore(matcher)

    buffer.readSlice(frameLength)
    // if (frameLength < 0) {
    //   state.cont(this)
    // } else {
    //   state.ret(buffer.readSlice(frameLength))
    // }
  }
}

class ConsumingDelimiterParser(matcher: Matcher) extends Parser[ChannelBuffer] {

  def this(bytes: Array[Byte]) = this(new DelimiterMatcher(bytes))

  def this(string: String) = this(new DelimiterMatcher(string))

  def decodeRaw(buffer: ChannelBuffer) = {
    val frameLength = buffer.bytesBefore(matcher)

    val frame = buffer.readSlice(frameLength)
    buffer.skipBytes(matcher.bytesMatching(buffer, buffer.readerIndex))
    frame

    // if (frameLength < 0) {
    //   state.cont(this)
    // } else {
    //   val frame = buffer.readSlice(frameLength)
    //   buffer.skipBytes(matcher.bytesMatching(buffer, buffer.readerIndex))
    //   state.ret(frame)
    // }
  }
}
