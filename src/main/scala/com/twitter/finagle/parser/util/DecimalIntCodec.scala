package com.twitter.finagle.parser.util

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}


object DecimalIntCodec {
  private val AsciiZero   = 48.toByte
  private val MinIntBytes = Int.MinValue.toString.getBytes("US-ASCII")
  private val MaxStringLength = 11

  private def encodeToArray(int: Int, bytes: Array[Byte], offset: Int) = {
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
