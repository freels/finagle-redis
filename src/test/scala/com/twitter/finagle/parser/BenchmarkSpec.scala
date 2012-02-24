package com.twitter.finagle.parser.test

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util._


object IncrementalParserPerfSpec extends BenchmarkSpecification {
  "performance" in {
    import Parsers._
    import DecodingHelpers._

    val readInt = readLine into { bytes => liftOpt(decodeDecimalInt(bytes)) }
    val readBulk = Parsers.accept("$") and (readInt into { length =>
      readBytes(length) into { bytes =>
        readBytes(2) and success(bytes)
      }
    })

    val test1 = Parsers.accept("*") and (readInt into { count =>
      repN(count, readBulk)
    })

    val count = 100
    val buf1 = ChannelBuffers.wrappedBuffer(("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes)

    for (x <- 1 to 10) {
      benchmark("test 1", 10000) {
        buf1.resetReaderIndex
        test1.decode(buf1)
      }
    }
  }
}
