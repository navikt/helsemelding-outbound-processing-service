package no.nav.helsemelding.outbound.processing.client.scopedhttpclient

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
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.client.scopedhttpclient.config.AzureAuth
import no.nav.helsemelding.outbound.processing.client.scopedhttpclient.config.Config

fun scopedAuthHttpClient(config: Config): () -> HttpClient = {
    authenticatedHttpClient(
        tokenClient = tokenHttpClient(config),
        config = config
    )
}

private fun tokenHttpClient(config: Config): HttpClient =
    HttpClient(CIO) {
        install(HttpTimeout) {
            connectTimeoutMillis =
                config.httpTokenClient.connectionTimeout.inWholeMilliseconds
        }

        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
    }

private fun authenticatedHttpClient(
    tokenClient: HttpClient,
    config: Config
): HttpClient =
    HttpClient(CIO) {
        install(HttpTimeout) {
            connectTimeoutMillis =
                config.httpClient.connectionTimeout.inWholeMilliseconds
        }

        install(ContentNegotiation) {
            json(
                Json {
                    ignoreUnknownKeys = true
                    encodeDefaults = true
                }
            )
        }

        install(Auth) {
            bearer {
                refreshTokens {
                    val tokenInfo = submitTokenForm(
                        tokenClient = tokenClient,
                        azureAuth = config.azureAuth,
                        scope = config.service.scope
                    ).body<TokenInfo>()

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
        url = azureAuth.tokenEndpoint.toString(),
        formParameters = parameters {
            append("client_id", azureAuth.appClientId)
            append("client_secret", azureAuth.appClientSecret)
            append("grant_type", azureAuth.grantType)
            append("scope", scope)
        }
    )
