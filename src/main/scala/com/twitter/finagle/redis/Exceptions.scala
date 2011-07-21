package com.twitter.finagle.redis


class RedisException(message: String) extends Exception(message)

class UnexpectedServerError(message: String) extends RedisException(message)
class UnexpectedResponseError(message: String) extends RedisException(message)
