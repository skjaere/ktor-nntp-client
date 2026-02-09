package io.skjaere.nntp

open class NntpException(
    message: String,
    val response: NntpResponse? = null,
    cause: Throwable? = null
) : Exception(message, cause)

class NntpProtocolException(
    message: String,
    response: NntpResponse? = null
) : NntpException(message, response)

class NntpAuthenticationException(
    message: String,
    response: NntpResponse? = null
) : NntpException(message, response)

class NntpConnectionException(
    message: String,
    cause: Throwable? = null
) : NntpException(message, cause = cause)

open class YencDecodingException(
    message: String,
    cause: Throwable? = null
) : NntpException(message, cause = cause)

class YencCrcMismatchException(
    message: String,
    val expected: UInt,
    val actual: UInt
) : YencDecodingException(message)
