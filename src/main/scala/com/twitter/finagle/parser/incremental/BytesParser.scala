package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}


object BytesParser {
  val ChunkSize = 1024
}

class BytesParser(bytesLeft: Int, data: ChannelBuffer = null) extends Parser[ChannelBuffer] {

  import BytesParser._

  def decodeWithState(state: ParseState, buffer: ChannelBuffer) {
    if (data eq null) {
      if (buffer.readableBytes >= bytesLeft) {
        state.ret(buffer.readSlice(bytesLeft))

      } else if (buffer.readableBytes >= ChunkSize) {
        val newData = ChannelBuffers.buffer(bytesLeft)

        buffer.readBytes(newData, ChunkSize)
        state.cont(new BytesParser(bytesLeft - ChunkSize, newData))

      } else {
        state.cont(this)
      }
    } else {
      if (buffer.readableBytes >= bytesLeft) {
        var newLeft = bytesLeft - buffer.readableBytes
        if (newLeft < 0) newLeft = 0

        buffer.readBytes(data, bytesLeft - newLeft)
        state.ret(data)

      } else if (buffer.readableBytes >= ChunkSize) {
        buffer.readBytes(data, ChunkSize)
        state.cont(new BytesParser(bytesLeft - ChunkSize, data))

      } else {
        state.cont(this)
      }
    }
  }
}

class SkipBytesParser(toRead: Int) extends Parser[Unit] {
  def decodeWithState(state: ParseState, buffer: ChannelBuffer) = {
    val readable = buffer.readableBytes

    if (readable < toRead) {
      buffer.skipBytes(readable)
      state.cont(new SkipBytesParser(toRead - readable))
    } else {
      buffer.skipBytes(toRead)
      state.ret(())
    }
  }
}
