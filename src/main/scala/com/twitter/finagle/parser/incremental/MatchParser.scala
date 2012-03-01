package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.Matcher


class ConsumingMatchParser(matcher: Matcher) extends Parser[Int] {
  def decodeRaw(buffer: ChannelBuffer) = {
    val size = matcher.bytesMatching(buffer, buffer.readerIndex)

    if (size < 0) {
      if (size == -1) {
        throw Continue(this)
      } else {
        throw Fail(() => "Matcher %s failed." format matcher)
      }
    }

    buffer.skipBytes(size)
    size
  }
}

class MatchParser(matcher: Matcher) extends Parser[Int] {
  def decodeRaw(buffer: ChannelBuffer) = {
    val size = matcher.bytesMatching(buffer, buffer.readerIndex)

    if (size < 0) {
      if (size == -1) {
        throw Continue(this)
      } else {
        throw Fail(() => "Matcher %s failed." format matcher)
      }
    }

    size
  }
}

class Match1ByteParser(byte: Byte) extends Parser[Int] {
  def decodeRaw(buffer: ChannelBuffer) = {
    try {
      val b = buffer.readByte
      if (byte != b) {
        buffer.readerIndex(buffer.readerIndex - 1)
        throw Fail(() => "Match for '%s' failed." format byte)
      }

      1
    } catch {
      case e: IndexOutOfBoundsException => throw Continue(this)
    }
  }
}


class Match2ByteParser(byte1: Byte, byte2: Byte) extends Parser[Int] {
  def decodeRaw(buffer: ChannelBuffer) = {
    val start = buffer.readerIndex

    try {
      val b1 = buffer.readByte
      if (byte1 != b1) {
        buffer.readerIndex(start)
        throw Fail(() => "Match for '%s%s' failed." format (byte1, byte2))
      }

      val b2 = buffer.readByte
      if (byte2 != b2) {
        buffer.readerIndex(start)
        throw Fail(() => "Match for '%s%s' failed." format (byte1, byte2))
      }

      2
    } catch {
      case e: IndexOutOfBoundsException =>
        buffer.readerIndex(start)
        throw Continue(this)
    }
  }
}
