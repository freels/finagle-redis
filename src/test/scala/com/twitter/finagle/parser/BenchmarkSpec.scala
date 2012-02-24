package com.twitter.finagle.parser.test

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util._


object Redis {
  import Parsers._
  import DecodingHelpers._

  val readInt = readLine flatMap { bytes =>
    liftOpt(decodeDecimalInt(bytes))
  }

  val readBulk = readInt flatMap { length =>
    readBytes(length) flatMap { bytes =>
      readBytes(2) then success(bytes)
    }
  }

  val readSingleBulk = accept("$") then readBulk

  val readMultiBulk = accept("*") then {
    readInt flatMap { count =>
      repN(count, readBulk)
    }
  }
}

object IncrementalParserPerfSpec extends BenchmarkSpecification {
  "performance" in {

    val count = 100
    val bytes = ("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes
    val buf1 = ChannelBuffers.wrappedBuffer(bytes)

    for (x <- 1 to 10) {
      benchmark("test 1", 10000) {
        buf1.resetReaderIndex
        readMultiBulk.decode(buf1)
      }
    }
  }
}
