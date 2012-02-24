package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer


class BacktrackingParser[+Out](inner: Parser[Out], offset: Int) extends Parser[Out] {

  def this(inner: Parser[Out]) = this(inner, 0)

  def decodeWithState(state: ParseState, buffer: ChannelBuffer) {
    val start = buffer.readerIndex

    buffer.readerIndex(start + offset)

    inner.decodeWithState(state, buffer)

    if (state.isCont) {
      if (state.cont == inner && buffer.readerIndex == (start + offset)) {
        buffer.readerIndex(start)
        state.cont(this)
      } else {
        val newOffset = buffer.readerIndex - start
        buffer.readerIndex(start)
        state.cont(new BacktrackingParser(state.cont, newOffset))
      }
    } else if (state.isFail) {
      buffer.readerIndex(start)
    } else if (state.isError) {
      buffer.readerIndex(start)
      state.fail(state.errorMessage)
    }
  }
}
