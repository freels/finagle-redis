package com.twitter.finagle.parser.util

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

  def decode(buf: ChannelBuffer): Int = {
    decode(buf, Int.MaxValue)
  }

  def decode(buf: ChannelBuffer, maxBytes: Int): Int = {
    var result = 0
    var sign   = 1
    var done   = false

    val c = buf.getByte(buf.readerIndex)

    if (c == '-') {
      sign = -1
    } else if (c != '+') {
      val n = c - AsciiZero

      if (0 <= n && n <= 9) {
        result *= 10
        result += n

      } else {
        done = true
      }
    }

    if (!done) buf.readByte

    var i = 1
    while (i < maxBytes && !done) {

      val n = buf.getByte(buf.readerIndex) - AsciiZero

      if (0 <= n && n <= 9) {
        result *= 10
        result += n
        buf.readByte

        i += 1

      } else {
        done = true
      }
    }

    result * sign
  }
}
