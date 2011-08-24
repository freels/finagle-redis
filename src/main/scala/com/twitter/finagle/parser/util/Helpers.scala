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

  def decodeFlags(num: Byte): Array[Boolean] = {
    val flags = new Array[Boolean](8)
    var i     = 0

    do {
      flags(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < flags.length)

    flags
  }

  def decodeFlags(num: Short): Array[Boolean] = {
    val flags = new Array[Boolean](16)
    var i     = 0

    do {
      flags(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < flags.length)

    flags
  }

  def decodeFlags(num: Int): Array[Boolean] = {
    val flags = new Array[Boolean](32)
    var i     = 0

    do {
      flags(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < flags.length)

    flags
  }

  def decodeFlags(num: Long): Array[Boolean] = {
    val flags = new Array[Boolean](64)
    var i     = 0

    do {
      flags(i) = (num & (1 << i)) != 0
      i = i + 1
    } while (i < flags.length)

    flags
  }
}
