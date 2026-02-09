package io.skjaere.nntp

internal object NntpCommand {
    const val CAPABILITIES = "CAPABILITIES"
    const val MODE_READER = "MODE READER"
    const val NEXT = "NEXT"
    const val LAST = "LAST"
    const val QUIT = "QUIT"
    const val HELP = "HELP"
    const val DATE = "DATE"
    const val POST = "POST"

    fun article(messageId: String): String = "ARTICLE $messageId"
    fun article(number: Long): String = "ARTICLE $number"

    fun body(messageId: String): String = "BODY $messageId"
    fun body(number: Long): String = "BODY $number"

    fun head(messageId: String): String = "HEAD $messageId"
    fun head(number: Long): String = "HEAD $number"

    fun stat(messageId: String): String = "STAT $messageId"
    fun stat(number: Long): String = "STAT $number"

    fun group(name: String): String = "GROUP $name"

    fun listGroup(name: String?, range: LongRange?): String = buildString {
        append("LISTGROUP")
        if (name != null) {
            append(" $name")
            if (range != null) {
                append(" ${range.first}-${range.last}")
            }
        }
    }

    fun list(keyword: String?, vararg arguments: String): String = buildString {
        append("LIST")
        if (keyword != null) {
            append(" $keyword")
            arguments.forEach { append(" $it") }
        }
    }

    fun over(range: LongRange): String = "OVER ${range.first}-${range.last}"
    fun over(messageId: String): String = "OVER $messageId"

    fun xover(range: LongRange): String = "XOVER ${range.first}-${range.last}"

    fun hdr(field: String, range: LongRange): String = "HDR $field ${range.first}-${range.last}"
    fun hdr(field: String, messageId: String): String = "HDR $field $messageId"

    fun xhdr(field: String, range: LongRange): String = "XHDR $field ${range.first}-${range.last}"

    fun newGroups(date: String, time: String, gmt: Boolean): String = buildString {
        append("NEWGROUPS $date $time")
        if (gmt) append(" GMT")
    }

    fun newNews(wildmat: String, date: String, time: String, gmt: Boolean): String = buildString {
        append("NEWNEWS $wildmat $date $time")
        if (gmt) append(" GMT")
    }

    fun authinfoUser(username: String): String = "AUTHINFO USER $username"
    fun authinfoPass(password: String): String = "AUTHINFO PASS $password"

    fun ihave(messageId: String): String = "IHAVE $messageId"
}
