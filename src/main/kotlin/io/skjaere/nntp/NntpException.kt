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

class NntpConnectionException(
    message: String,
    cause: Throwable? = null
) : NntpException(message, cause = cause)

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
