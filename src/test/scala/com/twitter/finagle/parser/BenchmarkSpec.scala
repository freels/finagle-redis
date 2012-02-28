package com.twitter.finagle.parser.test

import org.specs.Specification
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util._


object Redis {
  import Parsers._
  import DecodingHelpers._

  def decodeInt(buf: ChannelBuffer): Int = {
    var sign     = 1
    var result   = 0

    var c = buf.readByte

    if (c == '-') {
      sign   = -1
    } else {
      result = result * 10 + (c - '0')
    }

    do {
      c = buf.readByte

      if (c != '\r') {
        result = result * 10 + (c - '0')
      } else {
        buf.readByte // \n
        return result * sign
      }
    } while (true)

    // not reached
    0
  }

  val skipCRLF       = skipBytes(2)
  val readInt        = withRawBuffer { decodeInt(_) }
  val readBulk       = readInt >>= { readBytes(_) through skipCRLF }
  val readSingleBulk = "$" >> readBulk
  val readMultiBulk  = "*" >> (readInt >>= { repN(_, readSingleBulk) })
}

object IncrementalParserPerfSpec extends BenchmarkSpecification {
  "performance" in {

    val count = 100
    val bytes = ("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes
    val buf1 = ChannelBuffers.wrappedBuffer(bytes)

    for (x <- 1 to 10) {
      benchmark("test 1", 100000) {
        buf1.resetReaderIndex
        Redis.readMultiBulk.decode(buf1)
      }
    }
  }
}
