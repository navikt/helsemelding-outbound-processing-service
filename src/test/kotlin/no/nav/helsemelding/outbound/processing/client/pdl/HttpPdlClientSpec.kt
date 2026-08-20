package no.nav.helsemelding.outbound.processing.client.pdl

import arrow.core.getOrElse
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.StringSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.auth.AccessTokenProvider
import no.nav.helsemelding.outbound.processing.client.pdl.model.ErrorExtension
import no.nav.helsemelding.outbound.processing.client.pdl.model.GraphQlError
import no.nav.helsemelding.outbound.processing.client.pdl.model.GraphQlResponseError
import no.nav.helsemelding.outbound.processing.client.pdl.model.HttpError
import no.nav.helsemelding.outbound.processing.client.pdl.model.Person
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonData
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonName
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonNotFound
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonRequest
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonResponse
import no.nav.helsemelding.outbound.processing.client.pdl.model.UnexpectedError
import no.nav.helsemelding.outbound.processing.model.ErrorCode

private const val PROCESSING_NUMBER_HEADER_KEY = "Behandlingsnummer"
private const val PROCESSING_NUMBER_HEADER_VALUE = "B123"
private const val ACCESS_TOKEN = "access-token"
private const val PDL_SCOPE = "pdl-scope"

class HttpPdlClientSpec : StringSpec(
    {
        val personident = personident("12345678901")

        "status OK and valid data should return requested name of person" {
            val name = PersonName(
                firstName = "Ola",
                middleName = "Jens",
                lastName = "Nordmann"
            )
            val personResponse = PersonResponse(
                errors = emptyList(),
                data = PersonData(
                    person = Person(
                        names = listOf(name)
                    )
                )
            )
            val client = testClient { request ->
                request.shouldBePdlPersonRequest(personident)

                respond(
                    content = Json.encodeToString(personResponse),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                )
            }

            val response = client.getPersonName(personident)

            val personName = response.shouldBeRight()
            personName shouldBe name
        }

        "status OK and errors not empty should return GraphQlError" {
            val personResponse = PersonResponse(
                errors = listOf(generateGraphQlResponseError("not_found")),
                data = null
            )
            val client = testClient { request ->
                request.shouldBePdlPersonRequest(personident)

                respond(
                    content = Json.encodeToString(personResponse),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                )
            }

            val response = client.getPersonName(personident)

            val error = response.shouldBeLeft()
            val personNotFoundError = error.shouldBeInstanceOf<GraphQlError>()
            personNotFoundError.code shouldBe ErrorCode.PDL_ERROR
            personNotFoundError.message shouldBe "Error while requesting person from PDL"
            personNotFoundError.errors.size shouldBe 1
        }

        withData(
            nameFn = { "status OK and ${it.first} should return PersonNotFound" },
            "data is null" to PersonResponse(
                errors = emptyList(),
                data = null
            ),
            "data.hentPerson is null" to PersonResponse(
                errors = emptyList(),
                data = PersonData(
                    person = null
                )
            )
        ) {
            val personResponse = it.second
            val client = testClient { request ->
                request.shouldBePdlPersonRequest(personident)

                respond(
                    content = Json.encodeToString(personResponse),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                )
            }

            val response = client.getPersonName(personident)

            val error = response.shouldBeLeft()
            val personNotFoundError = error.shouldBeInstanceOf<PersonNotFound>()
            personNotFoundError.code shouldBe ErrorCode.PDL_ERROR
            personNotFoundError.message shouldBe "Person not found"
        }

        "status OK and empty navn list should return PersonNotFound" {
            val personResponse = PersonResponse(
                errors = emptyList(),
                data = PersonData(
                    person = Person(
                        names = emptyList()
                    )
                )
            )
            val client = testClient { request ->
                request.shouldBePdlPersonRequest(personident)

                respond(
                    content = Json.encodeToString(personResponse),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                )
            }

            val response = client.getPersonName(personident)

            val error = response.shouldBeLeft()
            val personNotFoundError = error.shouldBeInstanceOf<PersonNotFound>()
            personNotFoundError.code shouldBe ErrorCode.PDL_ERROR
            personNotFoundError.message shouldBe "Person not found"
        }

        "status not OK should return HttpError" {
            val client = testClient {
                respond(
                    content = "Service Unavailable",
                    status = HttpStatusCode.ServiceUnavailable
                )
            }

            val response = client.getPersonName(personident)

            val error = response.shouldBeLeft()
            val fetchingError = error.shouldBeInstanceOf<HttpError>()
            fetchingError.code shouldBe ErrorCode.PDL_ERROR
            fetchingError.statusCode shouldBe HttpStatusCode.ServiceUnavailable.value
            fetchingError.message shouldBe "Service Unavailable"
        }

        "request failure should return UnexpectedError" {
            val client = testClient {
                throw RuntimeException("PDL unavailable")
            }

            val response = client.getPersonName(personident)

            val error = response.shouldBeLeft()
            val unexpectedError = error.shouldBeInstanceOf<UnexpectedError>()
            unexpectedError.code shouldBe ErrorCode.PDL_ERROR
            unexpectedError.message shouldBe "Failed to request person from PDL: PDL unavailable"
        }

        "status OK and invalid response body should return UnexpectedError" {
            val client = testClient {
                respond(
                    content = "not json",
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                )
            }

            shouldThrowAny {
                client.getPersonName(personident)
            }
        }
    }
)

private fun testClient(
    tokenProvider: AccessTokenProvider = FakeAccessTokenProvider(),
    handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData
): PdlClient = HttpPdlClient(
    pdlGraphqlUrl = "http://localhost",
    processingNumber = PROCESSING_NUMBER_HEADER_VALUE,
    tokenProvider = tokenProvider,
    scope = PDL_SCOPE,
    clientProvider = {
        HttpClient(MockEngine) {
            engine { addHandler(handler) }

            install(ContentNegotiation) {
                json(
                    Json {
                        ignoreUnknownKeys = true
                        encodeDefaults = true
                    }
                )
            }
        }
    }
)

private fun HttpRequestData.shouldBePdlPersonRequest(
    personident: Personident,
    accessToken: String = ACCESS_TOKEN
) {
    method shouldBe HttpMethod.Post
    headers[HttpHeaders.Authorization] shouldBe "Bearer $accessToken"
    headers[PROCESSING_NUMBER_HEADER_KEY] shouldBe PROCESSING_NUMBER_HEADER_VALUE

    val body = bodyAsText()
    body shouldContain """"navnHistorikk":false"""

    val pdlRequest = Json.decodeFromString<PersonRequest>(body)
    pdlRequest.variables.nationalIdentityNumber shouldBe personident.value
    pdlRequest.variables.includeNameHistory shouldBe false
}

private fun HttpRequestData.bodyAsText(): String =
    when (val outgoingContent = body) {
        is OutgoingContent.ByteArrayContent -> outgoingContent.bytes().decodeToString()
        else -> error("Unsupported request body: ${outgoingContent::class}")
    }

fun generateGraphQlResponseError(code: String? = null) =
    GraphQlResponseError(
        message = "Error",
        locations = emptyList(),
        path = emptyList(),
        extensions = ErrorExtension(
            code = code,
            classification = "Classification"
        )
    )

private fun personident(value: String): Personident =
    Personident(value).getOrElse { error(it.message) }

private class FakeAccessTokenProvider : AccessTokenProvider {
    override suspend fun token(scope: String): String {
        scope shouldBe PDL_SCOPE
        return ACCESS_TOKEN
    }
}
