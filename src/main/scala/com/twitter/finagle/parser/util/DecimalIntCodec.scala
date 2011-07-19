package com.twitter.finagle.parser.util

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}
import com.twitter.finagle.ParseException


object DecimalIntCodec {
  private val AsciiZero   = 48
  private val MinIntBytes = Int.MinValue.toString.getBytes("US-ASCII")

  def encode(int: Int): ChannelBuffer = {
    val rv = ChannelBuffers.buffer(11)
    encode(int, rv)
    rv
  }

  def encode(int: Int, dest: ChannelBuffer) {
    if (int > 0) {
      val bytes = new Array[Byte](10) // maximum length of an encoded positive integer
      var i = 0
      var n = int

      do {
        bytes(i) = (AsciiZero + (n % 10)).toByte
        n = n / 10
        i = i + 1
      } while (n > 0)

      do {
        i = i - 1
        dest.writeByte(bytes(i))
      } while (i > 0)

    } else if (int == 0) {
      dest.writeByte(0)
    } else {
      // special-case Int.MinValue, since abs(Int.MinValue) is too large for max int
      if (int == Int.MinValue) {
        dest.writeBytes(MinIntBytes)
      } else {
        dest.writeByte('-')
        encode(int * -1, dest)
      }
    }
  }

  def decode(buf: ChannelBuffer): Option[Int] = {
    decode(buf, buf.readableBytes)
  }

  def decode(buf: ChannelBuffer, numBytes: Int): Option[Int] = {
    val last  = numBytes - 1
    var i     = last
    var rv    = 0
    var lower = 0
    var isNegative = false

    if (buf.getByte(buf.readerIndex) == '-') {
      lower = 1
      isNegative = true
    } else if (buf.getByte(buf.readerIndex) == '+') {
      lower = 1
    }

    while (i >= lower) {
      val c = buf.getByte(buf.readerIndex + i) - AsciiZero

      if (c < 0 || c > 9) return None
      rv = rv + c * pow(10, last - i)
      i = i - 1
    }

    if (isNegative) Some(rv * -1) else Some(rv)
  }

  // helpers

  private def pow(x: Int, p: Int) = {
    var rv = 1
    var j  = 0

    while (j < p) {
      rv = rv * x
      j  = j + 1
    }

    rv
  }
}
