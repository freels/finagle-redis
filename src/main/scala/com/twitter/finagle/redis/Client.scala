package com.twitter.finagle.redis

import scala.collection.mutable.ArrayBuilder
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import com.twitter.util.{Duration, Time, Future}
import com.twitter.conversions.time._
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
  protected def send(params: Array[Command.Argument]) = {
    service(Command(params))
  }
}

trait Client {
  import Command._
  import Reply._

  protected def send(params: Array[Command.Argument]): Future[Reply]

  private def argsBuilder(size: Int) = {
    val b = Array.newBuilder[Command.Argument]; b.sizeHint(size); b
  }

  // keys

  def del(key: String, keys: String*) = {
    val b = argsBuilder(keys.size + 1)

    b += DEL
    b += key
    keys foreach { b += _ }

    send(b.result) flatMap expectInteger
  }

  def exists(key: String) = {
    send(Array(EXISTS, key)) flatMap expectBoolean
  }

  def expire(key: String, after: Duration) = {
    send(Array(EXPIRE, key, after.inSeconds)) flatMap expectBoolean
  }

  def expireAt(key: String, at: Time) = {
    send(Array(EXPIREAT, key, at.inSeconds)) flatMap expectBoolean
  }

  def keys(pattern: String) = {
    send(Array(KEYS, pattern)) flatMap expectNonNilMultiBulk
  }

  def move(key: String, db: String) = {
    send(Array(MOVE, key, db)) flatMap expectBoolean
  }

  def objectRefcount(key: String) = {
    send(Array(OBJECT, ObjectCommand.REFCOUT, key)) flatMap expectInteger
  }

  def objectEncoding(key: String) = {
    send(Array(OBJECT, ObjectCommand.ENCODING, key)) flatMap expectNonNilBulk
  }

  def objectIdleTime(key: String) = {
    send(Array(OBJECT, ObjectCommand.IDLETIME, key)) flatMap
    expectInteger map { _.seconds }
  }

  def persist(key: String) = {
    send(Array(PERSIST, key)) flatMap expectBoolean
  }

  def randomKey = {
    send(Array(RANDOMKEY)) flatMap expectBulk
  }

  def rename(from: String, to: String) = {
    send(Array(RENAME, from, to)) flatMap expectOK
  }

  def renameNx(from: String, to: String) = {
    send(Array(RENAMENX, from, to)) flatMap expectBoolean
  }

  //def sort. This command sucks.

  def ttl(key: String) = {
    send(Array(TTL, key)) flatMap expectInteger
  }

  def keyType(key: String) = {
    send(Array(TYPE, key)) flatMap expectStatus
  }


  // string/bytearray values

  def append
  def decr
  def decrBy
  def get
  def getBit
  def getRange
  def getSet
  def incr
  def incrBy
  def mGet
  def mSet
  def mSetNx
  def set
  def setBit
  def setEx
  def setNx
  def setRange
  def strlen


  // helpers

  private def asByteArray(buffer: ChannelBuffer) = {
    val dst = new Array[Byte](buffer.readableBytes)
    buffer.readBytes(dst)
    dst
  }

  private def asString(buffer: ChannelBuffer) = {
    buffer.toString(Charset.forName("UTF-8"))
  }

  private def errOrUnexpected(r: Reply) = r match {
    case Error(msg) => if (msg == "no such key") {
      Future.exception(new NoSuchKeyError)
    } else {
      Future.exception(new UnexpectedServerError(asString(msg)))
    }
    case _ => Future.exception(new UnexpectedResponseError(""))
  }

  private def expectStatus(r: Reply) = r match {
    case Status(msg) => msg
    case _           => errOrUnexpected(r)
  }

  private def expectOK(r: Reply) = {
    expectStatus(r) map { _ => () }
  }

  private def expectBulk(r: Reply) = r match {
    case Bulk(opt) => Future.value(opt map asByteArray)
    case _         => errOrUnexpected(r)
  }

  private def expectNonNilBulk(r: Reply) = {
    expectBulk(r) flatMap { opt =>
      opt.get
    } rescue { case e: NoSuchElementException =>
      Future.exception(new UnexpectedResponseError("Reply had nil value"))
    }
  }

  private def expectMultiBulk(r: Reply) = r match {
    case MultiBulk(opt) => Future.value {
      opt map { _ map { _ map asByteArray } }
    }
    case _ => errOrUnexpected(r)
  }

  private def expectNonNilMultiBulk(r: Reply) = {
    expectMultiBulk(r) flatMap { data =>
      Future(data.get map { _.get })
    } rescue { case e: NoSuchElementException =>
      Future.exception(new UnexpectedResponseError("Reply had nil value"))
    }
  }

  private def expectInteger(r: Reply) = r match {
    case Integer(i) => Future.value(i)
    case _          => errOrUnexpected(r)
  }

  private def expectBoolean(r: Reply) = r match {
    case Integer(1) => Future.value(true)
    case Integer(0) => Future.value(false)
    case _          => errOrUnexpected(r)
  }
}
