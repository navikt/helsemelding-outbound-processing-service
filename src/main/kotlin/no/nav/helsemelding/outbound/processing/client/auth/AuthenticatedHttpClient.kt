package no.nav.helsemelding.outbound.processing.client.auth

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.forms.submitForm
import io.ktor.client.statement.HttpResponse
import io.ktor.http.parameters
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.config.AzureAuth
import no.nav.helsemelding.outbound.processing.config.HttpClientConfig

internal fun httpTokenClient(httpClientConfig: HttpClientConfig): HttpClient =
    HttpClient(CIO) {
        install(HttpTimeout) {
            connectTimeoutMillis = httpClientConfig.connectionTimeout.inWholeMilliseconds
        }
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
    }

internal fun scopedAuthHttpClient(
    tokenClient: HttpClient,
    azureAuth: AzureAuth,
    httpClientConfig: HttpClientConfig,
    scope: String
): HttpClient =
    HttpClient(CIO) {
        install(HttpTimeout) {
            connectTimeoutMillis = httpClientConfig.connectionTimeout.inWholeMilliseconds
        }
        install(ContentNegotiation) { json() }
        install(Auth) {
            bearer {
                refreshTokens {
                    val tokenInfo: TokenInfo = submitTokenForm(tokenClient, azureAuth, scope).body()
                    BearerTokens(tokenInfo.accessToken, null)
                }
                sendWithoutRequest { true }
            }
        }
    }

private suspend fun submitTokenForm(
    tokenClient: HttpClient,
    azureAuth: AzureAuth,
    scope: String
): HttpResponse =
    tokenClient.submitForm(
        url = azureAuth.azureTokenEndpoint.value,
        formParameters = parameters {
            append("client_id", azureAuth.azureAppClientId.value)
            append("client_secret", azureAuth.azureAppClientSecret.value)
            append("grant_type", azureAuth.azureGrantType.value)
            append("scope", scope)
        }
    )

@Serializable
internal data class TokenInfo(
    @SerialName("access_token") val accessToken: String,
    @SerialName("expires_in") val expiresIn: Int,
    @SerialName("token_type") val tokenType: String
)
