package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util._

class DelimiterFinderParser(matcher: Matcher) extends Parser[Int] {
  def this(bytes: Array[Byte]) = this(new BytesMatcher(bytes))

  def this(string: String) = this(new BytesMatcher(string))

  def decodeRaw(buffer: ChannelBuffer) = {
    buffer.bytesBefore(matcher)
  }
}

class DelimiterParser(matcher: Matcher) extends Parser[ChannelBuffer] {

  def this(bytes: Array[Byte]) = this(new BytesMatcher(bytes))

  def this(string: String) = this(new BytesMatcher(string))

  def decodeRaw(buffer: ChannelBuffer) = {
    val frameLength = buffer.bytesBefore(matcher)

    if (frameLength < 0) throw Continue(this)

    buffer.readSlice(frameLength)
  }
}

class ConsumingDelimiterParser(matcher: Matcher) extends Parser[ChannelBuffer] {

  def this(bytes: Array[Byte]) = this(new BytesMatcher(bytes))

  def this(string: String) = this(new BytesMatcher(string))

  def decodeRaw(buffer: ChannelBuffer) = {
    val frameLength = buffer.bytesBefore(matcher)

    if (frameLength < 0) throw Continue(this)

    val rv = buffer.readSlice(frameLength)
    buffer.skipBytes(matcher.bytesMatching(buffer, buffer.readerIndex))
    rv
  }
}
