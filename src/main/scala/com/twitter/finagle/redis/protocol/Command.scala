package com.twitter.finagle.redis.protocol

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import com.twitter.finagle.parser.util.EncodingHelpers._

abstract class CommandEnum extends Enumeration {
  type Name = Value

  private[redis] lazy val byteRepresentations = values map { v =>
    v -> v.toString.getBytes("UTF-8")
  } toMap

  private[redis] implicit def command2Arg(c: Name): Command.Argument = {
    byteRepresentations(c)
  }
}

object Command extends CommandEnum {
  private[redis] val CRLF = "\r\n".getBytes("US-ASCII")

  private[redis] implicit def string2Arg(s: String): Command.Argument = {
    s.getBytes("UTF-8")
  }

  private[redis] implicit def int2Arg(i: Int): Command.Argument = {
    encodeDecimalIntToArray(i)
  }

  type Argument = Array[Byte]

  val APPEND, AUTH, BGWRITEAOF, BGSAVE, BLPOP, BRPOP, BRPOPLPUSH, CONFIG, DBSIZE, DEBUG, DECR, DECRBY, DEL, DISCARD, ECHO, EXISTS, EXPIRE, EXPIREAT, FLUSHALL, FLUSHDB, GET, GETBIT, GETRANGE, GETSET, HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HKEYS, HLEN, HMGET, HMSET, HSET, HSETNX, HVALS, INCR, INCRBY, INFO, KEYS, LASTSAVE, LINDEX, LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, MGET, MONITOR, MOVE, MSET, MSETNX, MULTI, OBJECT, PERSIST, PING, PSUBSCRIBE, PUBLISH, PUNSUBSCRIBE, QUIT, RANDOMKEY, RENAME, RENAMENX, RPOP, RPOPLPUSH, RPUSH, RPUSHX, SADD, SAVE, SCARD, SDIFF, SDIFFSTORE, SELECT, SET, SETBIT, SETEX, SETNX, SETRANGE, SHUTDOWN, SINTER, SINTERSTORE, SISMEMBER, SLAVEOF, SLOWLOG, SMEMBERS, SMOVE, SORT, SPOP, SRANDOMMEMBER, SREM, STRLEN, SUBSCRIBE, SUNION, SUNIONSTORE, SYNC, TTL, TYPE, UNSUBSCRIBE, UNWATCH, WATCH, ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERSTORE, ZRANGE, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZREVRANK, ZSCORE, ZUNIONSTORE = Value

  object ObjectCommand extends CommandEnum {
    val REFCOUT, ENCODING, IDLETIME = Value
  }

  object ConfigCommand extends CommandEnum {
    val GET, RESETSTAT, SET = Value
  }

  object DebugCommand extends CommandEnum {
    val OBJECT, SEGFAULT = Value
  }
}


case class Command(params: Array[Command.Argument])

class CommandEncoder extends OneToOneEncoder {
  private def writeArgumentCount(buffer: ChannelBuffer, count: Int) {
    buffer.writeByte('*')
    encodeDecimalInt(count, buffer)
    buffer.writeBytes(Command.CRLF)
  }

  private def writeArgument(buffer: ChannelBuffer, arg: Array[Byte]) {
    buffer.writeByte('$')
    encodeDecimalInt(arg.length, buffer)
    buffer.writeBytes(Command.CRLF)
    buffer.writeBytes(arg)
    buffer.writeBytes(Command.CRLF)
  }

  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    val Command(arguments) = message
    val buffer = ChannelBuffers.dynamicBuffer(10 * arguments.length)

    writeArgumentCount(buffer, arguments.size)

    arguments foreach { writeArgument(buffer, _) }

    buffer
  }
}
