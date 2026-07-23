package no.nav.helsemelding.outbound.processing.client.pdl

import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe
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
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import no.nav.helsemelding.message.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.HttpPdlClient.Companion.BEHANDLINGSNUMMER_HEADER_KEY
import no.nav.helsemelding.outbound.processing.client.pdl.HttpPdlClient.Companion.BEHANDLINGSNUMMER_HEADER_VALUE
import no.nav.helsemelding.outbound.processing.client.pdl.model.FetchingError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlErrorExtension
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlHentPerson
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlPerson
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlPersonNavn
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlPersonResponse
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonNotFound
import no.nav.helsemelding.outbound.processing.client.pdl.model.QueryError

class HttpPdlClientSpec : StringSpec({

    val personident = Personident("12345678901")

    "status OK and valid data should return requested name of person" {
        val name = PdlPersonNavn(
            fornavn = "Ola",
            mellomnavn = "Jens",
            etternavn = "Nordmann"
        )
        val pdlPersonResponse = PdlPersonResponse(
            errors = emptyList(),
            data = PdlHentPerson(
                hentPerson = PdlPerson(
                    navn = listOf(name)
                )
            )
        )
        val client = testClient { request ->
            request.method shouldBe HttpMethod.Post
            request.headers[BEHANDLINGSNUMMER_HEADER_KEY] shouldBe BEHANDLINGSNUMMER_HEADER_VALUE

            respond(
                content = Json.encodeToString(pdlPersonResponse),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getPersonName(personident)

        val pdlPersonNavn = response.shouldBeRight()
        pdlPersonNavn shouldBe name
    }

    "status OK and errors not empty should return QueryError" {
        val pdlPersonResponse = PdlPersonResponse(
            errors = listOf(generatePdlError("not_found")),
            data = null
        )
        val client = testClient { request ->
            request.method shouldBe HttpMethod.Post
            request.headers[BEHANDLINGSNUMMER_HEADER_KEY] shouldBe BEHANDLINGSNUMMER_HEADER_VALUE

            respond(
                content = Json.encodeToString(pdlPersonResponse),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getPersonName(personident)

        val error = response.shouldBeLeft()
        val personNotFoundError = error.shouldBeInstanceOf<QueryError>()
        personNotFoundError.message shouldBe "Error while requesting person from PersonDataLosningen"
        personNotFoundError.errors!!.size shouldBe 1
    }

    withData(
        nameFn = { "status OK and ${it.first} should return PersonNotFound" },
        "data is null" to PdlPersonResponse(
            errors = emptyList(),
            data = null
        ),
        "data.hentPerson is null" to PdlPersonResponse(
            errors = emptyList(),
            data = PdlHentPerson(
                hentPerson = null
            )
        )
    ) {
        val pdlPersonResponse = it.second
        val client = testClient { request ->
            request.method shouldBe HttpMethod.Post
            request.headers[BEHANDLINGSNUMMER_HEADER_KEY] shouldBe BEHANDLINGSNUMMER_HEADER_VALUE

            respond(
                content = Json.encodeToString(pdlPersonResponse),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getPersonName(personident)

        val error = response.shouldBeLeft()
        val personNotFoundError = error.shouldBeInstanceOf<PersonNotFound>()
        personNotFoundError.message shouldBe "Person not found: data or hentPerson is null"
    }

    "status OK and empty navn list should return PersonNotFound" {
        val pdlPersonResponse = PdlPersonResponse(
            errors = emptyList(),
            data = PdlHentPerson(
                hentPerson = PdlPerson(
                    navn = emptyList()
                )
            )
        )
        val client = testClient { request ->
            request.method shouldBe HttpMethod.Post
            request.headers[BEHANDLINGSNUMMER_HEADER_KEY] shouldBe BEHANDLINGSNUMMER_HEADER_VALUE

            respond(
                content = Json.encodeToString(pdlPersonResponse),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getPersonName(personident)

        val error = response.shouldBeLeft()
        val personNotFoundError = error.shouldBeInstanceOf<PersonNotFound>()
        personNotFoundError.message shouldBe "Person not found: navn empty"
    }

    "status not OK should return FetchingError" {
        val client = testClient {
            respond(
                content = "Service Unavailable",
                status = HttpStatusCode.ServiceUnavailable
            )
        }

        val response = client.getPersonName(personident)

        val error = response.shouldBeLeft()
        val fetchingError = error.shouldBeInstanceOf<FetchingError>()
        fetchingError.code shouldBe HttpStatusCode.ServiceUnavailable.value
        fetchingError.message shouldBe "Service Unavailable"
    }
})

private fun testClient(
    handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData
): PdlClient = HttpPdlClient(
    pdlServiceUrl = "http://localhost",
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

fun generatePdlError(code: String? = null) =
    PdlError(
        message = "Error",
        locations = emptyList(),
        path = emptyList(),
        extensions = PdlErrorExtension(
            code = code,
            classification = "Classification"
        )
    )
