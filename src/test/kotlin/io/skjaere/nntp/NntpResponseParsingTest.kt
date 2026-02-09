package io.skjaere.nntp

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class NntpResponseParsingTest {

    @Test
    fun `parseResponseLine parses welcome message`() {
        val response = parseResponseLine("200 service available")
        assertEquals(200, response.code)
        assertEquals("service available", response.message)
    }

    @Test
    fun `parseResponseLine parses error response`() {
        val response = parseResponseLine("500 Command not recognized")
        assertEquals(500, response.code)
        assertEquals("Command not recognized", response.message)
    }

    @Test
    fun `parseResponseLine parses code-only response`() {
        val response = parseResponseLine("200 ")
        assertEquals(200, response.code)
    }

    @Test
    fun `parseResponseLine throws on invalid line`() {
        assertThrows<NntpProtocolException> { parseResponseLine("XX") }
    }

    @Test
    fun `parseResponseLine throws on non-numeric code`() {
        assertThrows<NntpProtocolException> { parseResponseLine("abc Invalid") }
    }

    @Test
    fun `parseArticleResponseLine parses article response`() {
        val (code, number, messageId) = parseArticleResponseLine("220 42 <msg@example.com> article follows")
        assertEquals(220, code)
        assertEquals(42L, number)
        assertEquals("<msg@example.com>", messageId)
    }

    @Test
    fun `parseArticleResponseLine parses stat response`() {
        val (code, number, messageId) = parseArticleResponseLine("223 1 <test@host>")
        assertEquals(223, code)
        assertEquals(1L, number)
        assertEquals("<test@host>", messageId)
    }

    @Test
    fun `parseGroupResponseLine parses group response`() {
        val response = parseGroupResponseLine("211 5 1 5 test.group")
        assertEquals(211, response.code)
        assertEquals(5L, response.count)
        assertEquals(1L, response.low)
        assertEquals(5L, response.high)
        assertEquals("test.group", response.name)
    }

    @Test
    fun `parseGroupResponseLine handles large numbers`() {
        val response = parseGroupResponseLine("211 1000000 1 1000000 alt.binaries.test")
        assertEquals(211, response.code)
        assertEquals(1000000L, response.count)
        assertEquals(1L, response.low)
        assertEquals(1000000L, response.high)
        assertEquals("alt.binaries.test", response.name)
    }
}
