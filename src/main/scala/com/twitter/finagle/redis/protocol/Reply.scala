package com.twitter.finagle.redis.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util.DecodingHelpers._


sealed abstract class Reply

object Reply {
  case class Status(message: ChannelBuffer) extends Reply
  case class Error(message: ChannelBuffer) extends Reply
  case class Integer(integer: Int) extends Reply
  case class Bulk(data: Option[ChannelBuffer]) extends Reply
  case class MultiBulk(data: Option[Seq[Bulk]]) extends Reply
}

object ReplyDecoder {
  import Reply._
  import Parsers._

  private val skipCRLF = skipBytes(2)

  private val readInt = readLine into { b => liftOpt(decodeDecimalInt(b)) }


  private val readStatusReply = "+" then readLine map { Status(_) }

  private val readErrorReply = "-" then readLine map { Error(_) }

  private val readIntegerReply = ":" then readInt map { Integer(_) }

  private val readBulkReply = "$" then readInt into { size =>
    if (size < 0) {
      success(Bulk(None))
    } else {
      readBytes(size) map { b => Bulk(Some(b)) } through skipCRLF
    }
  }

  private val readMultiBulkReply = "*" then readInt into { count =>
    if (count < 0) {
      success(MultiBulk(None))
    } else {
      repN(count, readBulkReply) map { bs => MultiBulk(Some(bs)) }
    }
  }

  val parser = {
    readBulkReply      or
    readMultiBulkReply or
    readStatusReply    or
    readIntegerReply   or
    readErrorReply
  }
}

class ReplyDecoder extends ParserDecoder[Reply](ReplyDecoder.parser)
