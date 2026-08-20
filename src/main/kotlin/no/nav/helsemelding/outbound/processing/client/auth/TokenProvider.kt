package no.nav.helsemelding.outbound.processing.client.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import no.nav.helsemelding.outbound.processing.config.AzureAuth
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

private val log = KotlinLogging.logger {}
private val TOKEN_REFRESH_MARGIN = 60.seconds

interface AccessTokenProvider {
    suspend fun token(scope: String): String
}

class AzureTokenProvider(
    private val tokenClient: HttpClient,
    private val azureAuth: AzureAuth,
    private val clock: Clock = Clock.System
) : AccessTokenProvider {
    private var cachedToken: CachedToken? = null

    override suspend fun token(scope: String): String =
        cachedToken
            ?.takeUnless { it.expiresSoon(clock) }
            ?.accessToken
            ?: fetchAndCacheToken(scope).accessToken

    private suspend fun fetchAndCacheToken(scope: String): CachedToken =
        submitTokenForm(tokenClient, azureAuth, scope)
            .body<TokenInfo>()
            .let { tokenInfo ->
                tokenInfo.toCachedToken(clock)
                    .also { token -> cachedToken = token }
                    .also { tokenInfo.logFetched(scope) }
            }
}

private fun TokenInfo.logFetched(scope: String) {
    log.info { "Fetched Azure token for scope=$scope expiresIn=${expiresIn}s" }
}

private fun TokenInfo.toCachedToken(clock: Clock): CachedToken =
    CachedToken(
        accessToken = accessToken,
        expiresAt = clock.now() + expiresIn.seconds
    )

private data class CachedToken(
    val accessToken: String,
    val expiresAt: Instant
) {
    fun expiresSoon(clock: Clock): Boolean =
        expiresAt <= clock.now() + TOKEN_REFRESH_MARGIN
}
