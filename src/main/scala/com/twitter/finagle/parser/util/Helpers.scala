package com.twitter.finagle.parser.util

import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffer


object EncodingHelpers {
  def encodeDecimalInt(int: Int) = {
    DecimalIntCodec.encode(int)
  }

  def encodeDecimalInt(int: Int, dest: ChannelBuffer) {
    DecimalIntCodec.encode(int, dest)
  }

  def encodeDecimalIntToArray(int: Int) = {
    DecimalIntCodec.encodeArray(int)
  }
}


object DecodingHelpers {
  def decodeUTF8String(buf: ChannelBuffer) = {
    buf.toString(Charset.forName("UTF-8"))
  }

  def decodeDecimalInt(buf: ChannelBuffer) = {
    DecimalIntCodec.decode(buf)
  }

  def decodeDecimalInt(buf: ChannelBuffer, numBytes: Int) = {
    DecimalIntCodec.decode(buf, numBytes)
  }

  def decodeBits(num: Byte): Array[Boolean] = {
    val bits = new Array[Boolean](8)
    var i     = 0

    do {
      bits(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < bits.length)

    bits
  }

  def decodeBits(num: Short): Array[Boolean] = {
    val bits = new Array[Boolean](16)
    var i     = 0

    do {
      bits(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < bits.length)

    bits
  }

  def decodeBits(num: Int): Array[Boolean] = {
    val bits = new Array[Boolean](32)
    var i     = 0

    do {
      bits(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < bits.length)

    bits
  }

  def decodeBits(num: Long): Array[Boolean] = {
    val bits = new Array[Boolean](64)
    var i     = 0

    do {
      bits(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < bits.length)

    bits
  }
}
