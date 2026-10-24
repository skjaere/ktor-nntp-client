package io.skjaere.nntp

/**
 * An article's =ybegin and =ypart lines. [parse] throws a [YencDecodingException] naming the field for a number it
 * can't read (empty, or not a number) in [size], [part], [total], [partBegin] or [partEnd]: the copy is malformed, and
 * another provider's may be good. [line] is only copied, never used, so a value it can't read is taken as 0.
 */
data class YencHeaders(
    val line: Int,
    val size: Long,
    val name: String,
    val part: Int? = null,
    val total: Int? = null,
    val partBegin: Long? = null,
    val partEnd: Long? = null
) {
    companion object {
        fun parse(ybeginLine: String, ypartLine: String? = null): YencHeaders {
            val params = ybeginLine.removePrefix("=ybegin ")

            // name is always the last field and may contain spaces
            val nameIdx = params.indexOf(" name=")
            val beforeName = if (nameIdx >= 0) params.substring(0, nameIdx) else params
            val name = if (nameIdx >= 0) params.substring(nameIdx + 6) else ""

            val fields = mutableMapOf<String, String>()
            beforeName.split(" ")
                .filter { it.contains("=") }
                .forEach { token ->
                    val (key, value) = token.split("=", limit = 2)
                    fields[key] = value
                }

            var partBegin: Long? = null
            var partEnd: Long? = null
            if (ypartLine != null) {
                val partParams = ypartLine.removePrefix("=ypart ")
                partParams.split(" ")
                    .filter { it.contains("=") }
                    .forEach { token ->
                        val (key, value) = token.split("=", limit = 2)
                        when (key) {
                            "begin" -> partBegin = number(key, value, "=ypart", String::toLongOrNull)
                            "end" -> partEnd = number(key, value, "=ypart", String::toLongOrNull)
                        }
                    }
            }

            return YencHeaders(
                line = (fields["line"] ?: throw YencDecodingException("Missing 'line' in =ybegin")).toIntOrNull() ?: 0,
                size = fields["size"]?.let { number("size", it, "=ybegin", String::toLongOrNull) }
                    ?: throw YencDecodingException("Missing 'size' in =ybegin"),
                name = name,
                part = fields["part"]?.let { number("part", it, "=ybegin", String::toIntOrNull) },
                total = fields["total"]?.let { number("total", it, "=ybegin", String::toIntOrNull) },
                partBegin = partBegin,
                partEnd = partEnd
            )
        }

        private fun <T : Any> number(field: String, value: String, line: String, read: (String) -> T?): T =
            read(value) ?: throw YencDecodingException("Unreadable '$field' in $line: \"$value\"")
    }
}

data class YencTrailer(
    val size: Long,
    val crc32: UInt? = null,
    val pcrc32: UInt? = null,
    val part: Int? = null
) {
    companion object {
        fun parse(yendLine: String): YencTrailer {
            val params = yendLine.removePrefix("=yend ")
            val fields = mutableMapOf<String, String>()
            params.split(" ")
                .filter { it.contains("=") }
                .forEach { token ->
                    val (key, value) = token.split("=", limit = 2)
                    fields[key] = value
                }

            return YencTrailer(
                size = fields["size"]?.toLong()
                    ?: throw YencDecodingException("Missing 'size' in =yend"),
                crc32 = fields["crc32"]?.removePrefix("0x")?.toUInt(16),
                pcrc32 = fields["pcrc32"]?.removePrefix("0x")?.toUInt(16),
                part = fields["part"]?.toInt()
            )
        }
    }
}
