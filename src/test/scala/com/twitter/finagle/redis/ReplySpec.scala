package com.twitter.finagle.redis.protocol

import org.specs.Specification
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.parser.incremental.{Return, Fail, Continue}
import com.twitter.finagle.parser.test._


object ReplySpec extends ParserSpecification {

  val parseExpectations = List(
    "+OK\r\n"             -> Reply.Status(Buffer("OK")),
    "-ERR\r\n"            -> Reply.Error(Buffer("ERR")),
    ":123\r\n"            -> Reply.Integer(123),
    ":-123\r\n"           -> Reply.Integer(-123),
    ":0\r\n"              -> Reply.Integer(0),
    "$-1\r\n"             -> Reply.Bulk(None),
    "$0\r\n\r\n"          -> Reply.Bulk(Some(Buffer(""))),
    "$3\r\nfoo\r\n"       -> Reply.Bulk(Some(Buffer("foo"))),
    "*-1\r\n"             -> Reply.MultiBulk(None),
    "*0\r\n"              -> Reply.MultiBulk(Some(Seq())),
    "*1\r\n$-1\r\n"       -> Reply.MultiBulk(Some(Seq(Reply.Bulk(None)))),
    "*1\r\n$0\r\n\r\n"    -> Reply.MultiBulk(Some(Seq(Reply.Bulk(Some(Buffer("")))))),
    "*1\r\n$3\r\nfoo\r\n" -> Reply.MultiBulk(Some(Seq(Reply.Bulk(Some(Buffer("foo"))))))
  )

  "parser" in {
    for ((input, expected) <- parseExpectations) {
      input.replaceAll("\r\n", "\\\\r\\\\n") in {
        ReplyDecoder.parser mustParse input andReturn(expected)
      }
    }
  }
}
