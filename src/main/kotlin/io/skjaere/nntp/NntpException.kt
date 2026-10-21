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

class ArticleNotFoundException(
    message: String,
    response: NntpResponse? = null
) : NntpException(message, response)

class NntpAuthenticationException(
    message: String,
    response: NntpResponse? = null,
    cause: Throwable? = null
) : NntpException(message, response, cause)

open class NntpConnectionException(
    message: String,
    cause: Throwable? = null
) : NntpException(message, cause = cause)

/**
 * An established connection lost under a command: reset, cut off by another I/O error, or closed by the server in the
 * middle of a line or of an article's data. Its cause is the I/O error, when there was one. A connect that fails, the
 * read timeout and a connection closed by [NntpConnection.close] stay plain [NntpConnectionException]s, so a caller can
 * tell a dropped connection from a server it can't reach.
 */
class NntpConnectionLostException(
    message: String,
    cause: Throwable? = null
) : NntpConnectionException(message, cause)

open class YencDecodingException(
    message: String,
    cause: Throwable? = null
) : NntpException(message, cause = cause)

/**
 * A body whose CRC, [actual], isn't the one its =yend line gives, [expected]: a damaged copy of the
 * article. Thrown once the article has been read to its end. [article] is the article, decoded whole,
 * when it was read whole ([NntpClient.bodyYencBytes]); null from the Body channel, whose reader had
 * its bytes as they came.
 */
class YencCrcMismatchException @JvmOverloads constructor(
    message: String,
    val expected: UInt,
    val actual: UInt,
    val article: YencArticle? = null,
) : YencDecodingException(message)
