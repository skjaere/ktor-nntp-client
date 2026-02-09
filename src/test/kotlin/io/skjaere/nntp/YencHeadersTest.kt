package io.skjaere.nntp

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNull

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
