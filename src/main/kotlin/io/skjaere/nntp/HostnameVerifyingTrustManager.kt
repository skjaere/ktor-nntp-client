package io.skjaere.nntp

import java.net.InetAddress
import java.net.UnknownHostException
import java.security.KeyStore
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.Locale
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

/**
 * Checks a TLS server's certificate: [delegate] checks it is trusted, then it must name [host]
 * among its subject alternative names, as a DNS name or, when [host] is an IP address, as that
 * address. The subject's common name doesn't count (RFC 6125 §6.4.4), as in browsers and
 * Android.
 *
 * The platform's TLS checks a name only when asked to (an endpoint identification algorithm),
 * by its own rules and in its own words; this check's words are the ones the app shows. Ktor
 * 3.4's TLS, used before, passed a certificate that lists no DNS names (or, for an IP address,
 * no IP addresses) whatever it was issued for.
 */
internal class HostnameVerifyingTrustManager(
    private val delegate: X509TrustManager,
    private val host: String
) : X509TrustManager {

    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String) {
        delegate.checkServerTrusted(chain, authType)
        val certificate = chain.firstOrNull() ?: throw CertificateException("The server sent no certificate")
        val names = certificate.subjectAlternativeNames.orEmpty().map { it[0] as Int to it[1].toString() }
        val address = ipAddress(host)
        val named = if (address != null) {
            names.any { (type, name) -> type == IP_ADDRESS && ipAddress(name) == address }
        } else {
            names.any { (type, name) -> type == DNS_NAME && dnsNameMatches(host, name) }
        }
        if (!named) {
            val listed = names.filter { (type) -> type == DNS_NAME || type == IP_ADDRESS }.map { it.second }
            val shown = listed.joinToString(limit = NAMES_IN_MESSAGE, truncated = "and ${listed.size - NAMES_IN_MESSAGE} more")
            throw CertificateException("The server's certificate is not for $host; it names ${shown.ifEmpty { "no host" }}")
        }
    }

    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String) =
        delegate.checkClientTrusted(chain, authType)

    override fun getAcceptedIssuers(): Array<X509Certificate> = delegate.acceptedIssuers

    companion object {
        // GeneralName types in getSubjectAlternativeNames (RFC 5280 §4.2.1.6)
        private const val DNS_NAME = 2
        private const val IP_ADDRESS = 7
        private const val NAMES_IN_MESSAGE = 5

        /** The platform's default trust manager: the system's certificate authorities, or what the app configured. */
        fun platformDefault(): X509TrustManager =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .apply { init(null as KeyStore?) }
                .trustManagers.filterIsInstance<X509TrustManager>().first()
    }
}

/**
 * Whether a certificate's DNS [name] covers [host]: equal, ignoring case and a trailing dot, or
 * a wildcard standing for the whole left-most label (`*.example.com` covers `news.example.com`
 * but not `example.com` or `a.news.example.com`), never for a top-level domain (`*.com`).
 */
internal fun dnsNameMatches(host: String, name: String): Boolean {
    val h = host.trimEnd('.').lowercase(Locale.ROOT)
    val n = name.trimEnd('.').lowercase(Locale.ROOT)
    if (!n.startsWith("*.")) return h == n
    val suffix = n.substring(1)
    val label = h.removeSuffix(suffix)
    return suffix.count { it == '.' } >= 2 && h.endsWith(suffix) && label.isNotEmpty() && '.' !in label
}

/**
 * [text] as an IP address if it is an IPv4 literal (four decimal parts) or an IPv6 literal,
 * bracketed or not; null for anything else. Never looks a name up.
 */
internal fun ipAddress(text: String): InetAddress? {
    val literal = text.removeSurrounding("[", "]")
    if (':' in literal) {
        // Only hex digits, colons and dots (an IPv4 tail) before a zone: the JDK and Android
        // parse that as an IPv6 literal and never look it up.
        val ipv6 = literal.substringBefore('%').all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' || it == ':' || it == '.' }
        if (!ipv6) return null
        return try {
            InetAddress.getByName(literal)
        } catch (_: UnknownHostException) {
            null
        }
    }
    val parts = literal.split('.')
    val valid = parts.size == 4 && parts.all { it.length in 1..3 && it.all(Char::isDigit) && it.toInt() <= 255 }
    return if (valid) InetAddress.getByAddress(parts.map { it.toInt().toByte() }.toByteArray()) else null
}
