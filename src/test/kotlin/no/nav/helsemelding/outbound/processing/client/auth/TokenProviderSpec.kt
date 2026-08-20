package no.nav.helsemelding.outbound.processing.client.auth

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.client.request.forms.FormDataContent
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpHeaders.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode.Companion.OK
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.config

class TokenProviderSpec : StringSpec(
    {
        "should reuse cached token while it is valid" {
            var tokenRequests = 0
            val provider = AzureTokenProvider(
                tokenClient = testTokenClient { request ->
                    tokenRequests++
                    request.shouldBeTokenRequest()
                    respondToken(accessToken = "access-token-$tokenRequests", expiresIn = 120)
                },
                azureAuth = config().azureAuth
            )

            provider.token(config().pdl.scope) shouldBe "access-token-1"
            provider.token(config().pdl.scope) shouldBe "access-token-1"

            tokenRequests shouldBe 1
        }

        "should fetch new token when cached token expires soon" {
            var tokenRequests = 0
            val provider = AzureTokenProvider(
                tokenClient = testTokenClient { request ->
                    tokenRequests++
                    request.shouldBeTokenRequest()
                    respondToken(accessToken = "access-token-$tokenRequests", expiresIn = 60)
                },
                azureAuth = config().azureAuth
            )

            provider.token(config().pdl.scope) shouldBe "access-token-1"
            provider.token(config().pdl.scope) shouldBe "access-token-2"

            tokenRequests shouldBe 2
        }
    }
)

private fun testTokenClient(
    handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData
): HttpClient =
    HttpClient(MockEngine) {
        engine { addHandler(handler) }
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
    }

private fun MockRequestHandleScope.respondToken(
    accessToken: String,
    expiresIn: Int
): HttpResponseData =
    respond(
        content = """
            {
                "access_token": "$accessToken",
                "expires_in": $expiresIn,
                "token_type": "Bearer"
            }
        """.trimIndent(),
        status = OK,
        headers = headersOf(ContentType, Json.toString())
    )

private fun HttpRequestData.shouldBeTokenRequest() {
    method shouldBe HttpMethod.Post
    url.toString() shouldBe config().azureAuth.azureTokenEndpoint.value

    val formParameters = (body as FormDataContent).formData
    formParameters["client_id"] shouldBe config().azureAuth.azureAppClientId.value
    formParameters["client_secret"] shouldBe config().azureAuth.azureAppClientSecret.value
    formParameters["grant_type"] shouldBe config().azureAuth.azureGrantType.value
    formParameters["scope"] shouldBe config().pdl.scope
}
