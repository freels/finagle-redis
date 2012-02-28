package com.twitter.finagle.parser.test

import org.specs.Specification
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util._


object Redis {
  import Parsers._
  import DecodingHelpers._

  trait Reply

  object Reply {
    case class Status(msg: ChannelBuffer) extends Reply
    case class Error(msg: ChannelBuffer) extends Reply
    case class Integer(i: Int) extends Reply
    case class Bulk(bytes: ChannelBuffer) extends Reply
    case class MultiBulk(bulks: Option[Seq[Bulk]]) extends Reply
  }

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

  val skipCRLF = skipBytes(2)
  val readInt  = withRawBuffer { decodeInt(_) }
  val readLine = readTo("\r\n")

  val readStatusReply    = "+" then readLine map { Reply.Status(_) }
  val readErrorReply     = "-" then readLine map { Reply.Error(_) }
  val readIntegerReply   = ":" then readInt map { Reply.Integer(_) }
  val readBulkReply      = "$" then readInt flatMap { readBytes(_) } thenSkip skipCRLF map { Reply.Bulk(_) }
  val readMultiBulkReply = "*" then readInt flatMap { count =>
    if (count == -1) success(null) else repN(count, readBulkReply)
  } map { bulks =>
    Reply.MultiBulk(Option(bulks))
  }

  val readReply: Parser[Reply] = {
    readStatusReply    or
    readIntegerReply   or
    readBulkReply      or
    readMultiBulkReply or
    readErrorReply
  }
}

object IncrementalParserPerfSpec extends BenchmarkSpecification {
  "performance" in {

    val count = 100
    val bytes = ("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes
    val buf1  = ChannelBuffers.wrappedBuffer(bytes)

    for (x <- 1 to 1000) {
      benchmark("test 1", 100000) {
        buf1.resetReaderIndex
        Redis.readReply.decode(buf1)
      }
    }
  }
}
