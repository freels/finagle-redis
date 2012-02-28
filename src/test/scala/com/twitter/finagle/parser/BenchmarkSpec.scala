package com.twitter.finagle.parser.test

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util._


object Redis {
  import Parsers._
  import DecodingHelpers._

  //val readInt = readLine map { decodeDecimalInt(_) }

  val readInt = withRawBuffer { buf =>
    var result = 0
    var sign   = 1

    var c = buf.readByte

    if (c == '-') {
      sign = -1
    } else if (c != '+') {
      result *= 10
      result += DecimalIntCodec.parseByte(c)
    }

    var done = false
    do {
      c = buf.readByte

      if (c == '\r') {
        buf.readByte
        done = true
      } else {
        result *= 10
        result += DecimalIntCodec.parseByte(c)
      }
    } while (!done)

    result * sign
  }

  val skipCRLF       = skipBytes(2)
  val readBulk       = readInt >>= { readBytes(_) through skipCRLF }
  val readSingleBulk = "$" >> readBulk
  val readMultiBulk  = "*" >> (readInt >>= { repN(_, readSingleBulk) })
}

object IncrementalParserPerfSpec extends BenchmarkSpecification {
  "performance" in {

    val count = 100
    val bytes = ("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes
    val buf1 = ChannelBuffers.wrappedBuffer(bytes)

    for (x <- 1 to 100) {
      benchmark("test 1", 100000) {
        buf1.resetReaderIndex
        Redis.readMultiBulk.decodeRaw(buf1)
      }
    }
  }
}
