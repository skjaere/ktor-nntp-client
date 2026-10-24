package io.skjaere.nntp

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class YencHeadersTest {

    @Test
    fun `parse single-part ybegin line`() {
        val headers = YencHeaders.parse("=ybegin line=128 size=123456 name=testfile.bin")
        assertEquals(128, headers.line)
        assertEquals(123456L, headers.size)
        assertEquals("testfile.bin", headers.name)
        assertNull(headers.part)
        assertNull(headers.total)
        assertNull(headers.partBegin)
        assertNull(headers.partEnd)
    }

    @Test
    fun `parse multipart ybegin line`() {
        val headers = YencHeaders.parse(
            "=ybegin part=1 total=3 line=128 size=500000 name=archive.rar",
            "=ypart begin=1 end=166667"
        )
        assertEquals(128, headers.line)
        assertEquals(500000L, headers.size)
        assertEquals("archive.rar", headers.name)
        assertEquals(1, headers.part)
        assertEquals(3, headers.total)
        assertEquals(1L, headers.partBegin)
        assertEquals(166667L, headers.partEnd)
    }

    @Test
    fun `parse filename with spaces`() {
        val headers = YencHeaders.parse("=ybegin line=128 size=100 name=my file with spaces.txt")
        assertEquals("my file with spaces.txt", headers.name)
    }

    @Test
    fun `parse throws on missing line field`() {
        assertThrows<YencDecodingException> {
            YencHeaders.parse("=ybegin size=100 name=test.bin")
        }
    }

    @Test
    fun `parse throws on missing size field`() {
        assertThrows<YencDecodingException> {
            YencHeaders.parse("=ybegin line=128 name=test.bin")
        }
    }

    // K55: a header with an empty or non-numeric number is a malformed article, reported as a YencDecodingException
    // that names the field, so a caller can take the copy as bad and try another provider's.

    @Test
    fun `an empty or non-numeric number in =ybegin names its field`() {
        for (field in listOf("size", "part", "total")) {
            for (value in listOf("", "12a")) {
                val fields = mapOf("part" to "1", "total" to "3", "line" to "128", "size" to "500000") + (field to value)
                val ybegin = "=ybegin " + fields.entries.joinToString(" ") { "${it.key}=${it.value}" } + " name=a.bin"
                val e = assertThrows<YencDecodingException>("$field=$value") {
                    YencHeaders.parse(ybegin, "=ypart begin=1 end=166667")
                }
                assertTrue("'$field'" in e.message.orEmpty(), "$field=$value: ${e.message}")
            }
        }
    }

    @Test
    fun `an empty or non-numeric number in =ypart names its field`() {
        for ((field, ypart) in listOf(
            "begin" to "=ypart begin= end=166667",
            "begin" to "=ypart begin=x1 end=166667",
            "end" to "=ypart begin=1 end=",
            "end" to "=ypart begin=1 end=16a667",
        )) {
            val e = assertThrows<YencDecodingException>(ypart) {
                YencHeaders.parse("=ybegin part=1 total=3 line=128 size=500000 name=a.bin", ypart)
            }
            assertTrue("'$field'" in e.message.orEmpty(), "$ypart: ${e.message}")
        }
    }

    @Test
    fun `an unreadable line length is read as 0, as nothing but a copy uses it`() {
        for (value in listOf("", "x")) {
            val headers = YencHeaders.parse(
                "=ybegin part=2 total=3 line=$value size=500000 name=a.bin",
                "=ypart begin=166668 end=333334"
            )
            assertEquals(0, headers.line)
            assertEquals(500000L, headers.size)
            assertEquals(2, headers.part)
            assertEquals(166668L, headers.partBegin)
            assertEquals(333334L, headers.partEnd)
        }
    }

    @Test
    fun `parse yend trailer with crc32`() {
        val trailer = YencTrailer.parse("=yend size=123456 crc32=1a2b3c4d")
        assertEquals(123456L, trailer.size)
        assertEquals(0x1a2b3c4du, trailer.crc32)
        assertNull(trailer.pcrc32)
        assertNull(trailer.part)
    }

    @Test
    fun `parse yend trailer with pcrc32 and part`() {
        val trailer = YencTrailer.parse("=yend size=50000 part=1 pcrc32=abcdef01 crc32=12345678")
        assertEquals(50000L, trailer.size)
        assertEquals(0x12345678u, trailer.crc32)
        assertEquals(0xabcdef01u, trailer.pcrc32)
        assertEquals(1, trailer.part)
    }

    @Test
    fun `parse yend throws on missing size`() {
        assertThrows<YencDecodingException> {
            YencTrailer.parse("=yend crc32=12345678")
        }
    }

    @Test
    fun `parse yend with 0x prefix on crc`() {
        val trailer = YencTrailer.parse("=yend size=100 crc32=0xaabbccdd")
        assertEquals(0xaabbccddu, trailer.crc32)
    }
}
