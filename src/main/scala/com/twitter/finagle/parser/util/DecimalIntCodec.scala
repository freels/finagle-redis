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

  def decode(buf: ChannelBuffer): Int = {
    decode(buf, buf.readableBytes)
  }

  def decode(buf: ChannelBuffer, numBytes: Int): Int = {
    val digits     = numBytes - 1 // assume a prefix here. we'll add it back later.
    var rv         = 0
    var isNegative = 1

    var c = buf.readByte

    if (c == '-') {
      isNegative = -1
    } else if (c != '+') {
      rv += digitToDecimal(c, digits)
    }

    var i = 1
    while (i <= digits) {
      rv += digitToDecimal(buf.readByte, digits - i)
      i += 1
    }

    rv * isNegative
  }

  // helpers

  @inline def digitToDecimal(d: Byte, exp: Int) = {
    val n = d - AsciiZero
    if (n < 0 || n > 9) throw new ParseException("Invalid decimal int")

    pow(10, exp) * n
  }

  @inline def pow(x: Int, p: Int) = {
    var rv = 1
    var j  = 0

    while (j < p) {
      rv = rv * x
      j  = j + 1
    }

    rv
  }
}
