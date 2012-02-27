package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer


class BacktrackingParser[+Out](inner: Parser[Out], offset: Int) extends Parser[Out] {

  def this(inner: Parser[Out]) = this(inner, 0)

  def decodeRaw(buffer: ChannelBuffer): Out = {
    val start = buffer.readerIndex

    buffer.readerIndex(start + offset)

    try inner.decodeRaw(buffer) catch {
      case Continue(rest) =>
        if (rest == inner && buffer.readerIndex == (start + offset)) {
          buffer.readerIndex(start)
          throw Continue(this)
        } else {
          val newOffset = buffer.readerIndex - start
          buffer.readerIndex(start)
          throw Continue(new BacktrackingParser(rest, newOffset))
        }
      case f: Fail =>
        buffer.readerIndex(start)
        throw f
      case Error(msg) =>
        buffer.readerIndex(start)
        throw new Fail(msg)
    }
  }
}
