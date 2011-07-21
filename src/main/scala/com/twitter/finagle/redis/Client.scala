package com.twitter.finagle.redis

import scala.collection.JavaConversions._
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.Service
import com.twitter.util.{Time, Future}
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.protocol._

object Client {
  /**
   * Construct a client from a single host.
   *
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client = apply(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(new Redis)
      .build()
  )

  def apply(service: Service[Command, Reply]) = {
    new ConnectedClient(service)
  }
}

protected class ConnectedClient(service: Service[Command, Reply]) extends Client {
  protected def send(name: Command.Name, args: Array[Command.Argument]) = {
    service(Command(name, args))
  }
}

trait Client {
  import Command._
  import Reply._

  protected def send(name: Command.Name, args: Array[Command.Argument]): Future[Reply]

  // keys





  // helpers

  protected def asByteArray(buffer: ChannelBuffer) = {
    val dst = new Array[Byte](buffer.readableBytes)
    buffer.readBytes(dst)
    dst
  }

  protected def asString(buffer: ChannelBuffer) = {
    buffer.toString(Charset.forName("UTF-8"))
  }

  protected def unexpectedReply(r: Reply) = r match {
    case Error(msg) => Future.exception(new UnexpectedServerError(asString(msg)))
    case _          => Future.exception(new UnexpectedResponseError(""))
  }

  protected def expectOK(r: Reply) = r match {
    case Status(msg) => Future.Unit
    case _           => unexpectedReply(r)
  }

  protected def expectBulk(r: Reply) = r match {
    case Bulk(opt) => Future.value(opt map asByteArray)
    case _         => unexpectedReply(r)
  }

  protected def expectInteger(r: Reply) = r match {
    case Integer(i) => Future.value(i)
    case _          => unexpectedReply(r)
  }

  protected def expectBoolean(r: Reply) = r match {
    case Integer(1) => Future.value(true)
    case Integer(0) => Future.value(false)
    case _          => unexpectedReply(r)
  }
}
