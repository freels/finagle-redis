package com.twitter.finagle.parser.incremental

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.frame.FrameDecoder
import com.twitter.finagle.ParseException


class ParserDecoder[+Output](parser: Parser[Output]) extends FrameDecoder {
  private[this] var state = parser

  def start() {
    state = parser
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
    state.decode(buffer) match {
      case e: Fail => {
        start()
        throw new ParseException(e.message)
      }
      case e: Error => {
        start()
        throw new ParseException(e.message)
      }
      case Return(out) => {
        start()
        out.asInstanceOf[AnyRef]
      }
      case Continue(next) => {
        state = next
        needData
      }
    }
  }

  private val needData = null
}
