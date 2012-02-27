package com.twitter.finagle.parser.test

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util._


object Redis {
  import Parsers._
  import DecodingHelpers._

  val readInt = readLine map { decodeDecimalInt(_) }

  val skipCRLF = skipBytes(2)

  val readBulk = readInt flatMap { length =>
    readBytes(length) through skipCRLF
  }

  val readSingleBulk = acceptString("$") then readBulk

  val readMultiBulk = acceptString("*") then {
    readInt flatMap { count =>
      repN(count, readSingleBulk)
    }
  }
}

object IncrementalParserPerfSpec extends BenchmarkSpecification {
  "performance" in {

    val count = 100
    val bytes = ("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes
    val buf1 = ChannelBuffers.wrappedBuffer(bytes)

    for (x <- 1 to 1000) {
      benchmark("test 1", 10000) {
        buf1.resetReaderIndex
        Redis.readMultiBulk.decode(buf1)
      }
    }
  }
}
