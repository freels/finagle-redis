package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}


object BytesParser {
  val ChunkSize = 1024
}

class BytesParser(bytesLeft: Int, data: ChannelBuffer = null) extends Parser[ChannelBuffer] {

  import BytesParser._

  def decodeRaw(buffer: ChannelBuffer) = {
    if (data eq null) {
      if (buffer.readableBytes >= bytesLeft) {
        buffer.readSlice(bytesLeft)

      } else if (buffer.readableBytes >= ChunkSize) {
        val newData = ChannelBuffers.buffer(bytesLeft)

        buffer.readBytes(newData, ChunkSize)
        throw Continue(new BytesParser(bytesLeft - ChunkSize, newData))

      } else {
        throw Continue(this)
      }
    } else {
      if (buffer.readableBytes >= bytesLeft) {
        var newLeft = bytesLeft - buffer.readableBytes
        if (newLeft < 0) newLeft = 0

        buffer.readBytes(data, bytesLeft - newLeft)
        data

      } else if (buffer.readableBytes >= ChunkSize) {
        buffer.readBytes(data, ChunkSize)
        throw Continue(new BytesParser(bytesLeft - ChunkSize, data))

      } else {
        throw Continue(this)
      }
    }
  }
}

class SkipBytesParser(toRead: Int) extends Parser[Unit] {
  import BytesParser._

  def decodeRaw(buffer: ChannelBuffer) {
    val readable = buffer.readableBytes

    if (readable < toRead) {
      if (readable >= ChunkSize) {
        buffer.skipBytes(readable)
        throw Continue(new SkipBytesParser(toRead - readable))
      } else {
        throw Continue(this)
      }
    } else {
      buffer.skipBytes(toRead)
    }
  }
}
