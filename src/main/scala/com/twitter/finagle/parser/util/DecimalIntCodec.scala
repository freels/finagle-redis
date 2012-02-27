package com.twitter.finagle.parser.util

import com.twitter.finagle.parser.ParseException
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}


object DecimalIntCodec {
  val AsciiZero   = 48.toByte
  val MinIntBytes = Int.MinValue.toString.getBytes("US-ASCII")
  val MaxStringLength = 11

  def encodeToArray(int: Int, bytes: Array[Byte], offset: Int) = {
    var i = offset
    var n = int

    do {
      bytes(i) = (AsciiZero + (n % 10)).toByte
      n = n / 10
      i = i + 1
    } while (n > 0)

    bytes
  }

  def encodeArray(int: Int): Array[Byte] = {
    if (int > 0) {
      encodeToArray(int, new Array(MaxStringLength), 0)

    } else if (int == 0) {
      Array(AsciiZero)

    } else if (int == Int.MinValue) {
      // special-case Int.MinValue, since abs(Int.MinValue) is too large for max int
      MinIntBytes

    } else {
      val bytes = new Array[Byte](MaxStringLength)
      bytes(0) = '-'
      encodeToArray(-int, bytes, 1)
    }
  }

  def encode(int: Int): ChannelBuffer = {
    val rv = ChannelBuffers.buffer(MaxStringLength)
    encode(int, rv)
    rv
  }


  def encode(int: Int, dest: ChannelBuffer) {
    dest.writeBytes(encodeArray(int))
  }

  @inline def decode(buf: ChannelBuffer): Int = {
    decode(buf, buf.readableBytes)
  }

  @inline def decode(buf: ChannelBuffer, numBytes: Int): Int = {
    var result = 0
    var sign   = 1

    val c = buf.readByte

    if (c == '-') {
      sign = -1
    } else if (c != '+') {
      result *= 10
      result += parseByte(c)
    }

    var i = 1
    while (i < numBytes) {
      result *= 10
      result += parseByte(buf.readByte)
      i += 1
    }

    result * sign
  }

  @inline def parseByte(b: Byte) = {
    val n = b - AsciiZero
    if (n < 0 || n > 9) throw new ParseException("Invalid integer char")
    n
  }
}
