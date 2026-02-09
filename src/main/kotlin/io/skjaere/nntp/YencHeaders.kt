package io.skjaere.nntp

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
                            "begin" -> partBegin = value.toLong()
                            "end" -> partEnd = value.toLong()
                        }
                    }
            }

            return YencHeaders(
                line = fields["line"]?.toInt()
                    ?: throw YencDecodingException("Missing 'line' in =ybegin"),
                size = fields["size"]?.toLong()
                    ?: throw YencDecodingException("Missing 'size' in =ybegin"),
                name = name,
                part = fields["part"]?.toInt(),
                total = fields["total"]?.toInt(),
                partBegin = partBegin,
                partEnd = partEnd
            )
        }
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
