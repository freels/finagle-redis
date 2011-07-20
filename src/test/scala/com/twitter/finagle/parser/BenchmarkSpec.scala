package com.twitter.finagle.parser.incremental

import org.specs.Specification
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.embedder._

import com.twitter.finagle.parser.util._
import com.twitter.finagle.memcached
import com.twitter.finagle.memcached_test


object BenchmarkSpec extends Specification {
  val memcachedDecoder = new DecoderEmbedder(
    new memcached.protocol.text.client.Decoder,
    new memcached.protocol.text.client.DecodingToResponse
  )

  val testDecoder = new DecoderEmbedder(
    new memcached_test.ResponseDecoder
  )

  val response = ChannelBuffers.wrappedBuffer(
    (("VALUE foo 0 30\r\n"+ ("a" * 30) +"\r\n") * 30 + "END\r\n").getBytes("ASCII")
  )

  // "handrolled" in {
  //   for (x <- 1 to 10) {
  //     val rv = time { for (i <- 1 to 10000) {
  //       response.resetReaderIndex
  //       memcachedDecoder.offer(response)
  //     } }

  //     println("handrolled: "+ rv +" ("+ (rv / 10000.0) +")")
  //   }
  // }

  "combinators" in {
    for (x <- 1 to 10) {
      val rv = time { for (i <- 1 to 10000) {
        response.resetReaderIndex
        testDecoder.offer(response)
      } }

      println("combinators: "+ rv +" ("+ (rv / 10000.0) +")")
    }
  }

  // "performance" in {
  //   import Parsers._
  //   import DecodingHelpers._

  //   val readInt = readLine into { bytes => lift(decodeDecimalInt(bytes)) }
  //   val readBulk = Parsers.accept("$") append (readInt into { length =>
  //     readBytes(length) into { bytes =>
  //       readBytes(2) append success(bytes)
  //     }
  //   })

  //   val test1 = Parsers.accept("*") append (readInt into { count =>
  //     repN(count, readBulk)
  //   })

  //   val count = 100
  //   val buf1 = ChannelBuffers.wrappedBuffer(("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes)

  //   for (x <- 1 to 10) {
  //     val rv = time { for (i <- 1 to 10000) {
  //       buf1.resetReaderIndex
  //       test1.decode(buf1)
  //     } }

  //     println("test 1: "+ rv +" ("+ (rv / 10000.0) +")")
  //   }
  // }

  // helpers

  def now = System.currentTimeMillis

  def time[T](f: => T) = { val s = now; f; val e = now; e - s }
}
