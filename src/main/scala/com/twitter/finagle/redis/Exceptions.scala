package com.twitter.finagle.redis


class RedisException(message: String) extends Exception(message)

class NoSuchKeyError extends RedisException("no such key")
class UnexpectedServerError(message: String) extends RedisException(message)
class UnexpectedResponseError(message: String) extends RedisException(message)
