package com.twitter.finagle.parser.incremental

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.frame.FrameDecoder
import com.twitter.finagle.parser.ParseException


class ParserDecoder[+Out](parser: Parser[Out]) extends FrameDecoder {
  private[this] var state = parser

  def reset() {
    state = parser
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
    state.decode(buffer) match {
      case e: Fail => {
        reset()
        throw new ParseException(e.message)
      }
      case e: Error => {
        reset()
        throw new ParseException(e.message)
      }
      case Return(out) => {
        reset()
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
