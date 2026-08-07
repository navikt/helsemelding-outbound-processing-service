package no.nav.helsemelding.outbound.processing.client.providerregistry

import arrow.core.getOrElse
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equality.shouldBeEqualUsingFields
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
import io.ktor.http.fullPath
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.messageconverter.msghead.model.provider.OrganisationNumber
import no.nav.helsemelding.messageconverter.msghead.model.provider.Provider
import no.nav.helsemelding.messageconverter.msghead.model.provider.ProviderCategory
import no.nav.helsemelding.messageconverter.msghead.model.provider.ProviderOffice
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.HttpError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.UnexpectedClientError
import java.time.OffsetDateTime
import kotlin.uuid.Uuid

class HttpProviderRegistryClientSpec : StringSpec({

    "status OK should return requested provider" {
        val providerId = Uuid.random()
        val testProvider = createProvider(providerId)

        val client = testClient { request ->
            request.method shouldBe HttpMethod.Get
            request.url.fullPath shouldBe "/api/v1/behandler/$providerId"

            respond(
                content = Json.encodeToString(testProvider),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getProvider(providerId)

        val provider = response.shouldBeRight()
        provider shouldBeEqualUsingFields testProvider
    }

    "status not OK should return HttpError" {
        val client = testClient {
            respond(
                content = "Provider not found",
                status = HttpStatusCode.NotFound
            )
        }

        val response = client.getProvider(Uuid.random())

        val error = response.shouldBeLeft()
        val httpError = error.shouldBeInstanceOf<HttpError>()
        httpError.statusCode shouldBe HttpStatusCode.NotFound.value
        httpError.message shouldBe "Provider not found"
    }

    "request failure should return UnexpectedClientError" {
        val client = testClient {
            throw RuntimeException("ProviderRegistry unavailable")
        }

        val response = client.getProvider(Uuid.random())

        val error = response.shouldBeLeft()
        val unexpectedError = error.shouldBeInstanceOf<UnexpectedClientError>()
        unexpectedError.message shouldBe "Failed to request provider from ProviderRegistry: ProviderRegistry unavailable"
    }

    "status OK and invalid response body should return UnexpectedClientError" {
        val client = testClient {
            respond(
                content = "not json",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getProvider(Uuid.random())

        val error = response.shouldBeLeft()
        error.shouldBeInstanceOf<UnexpectedClientError>()
    }
})

private fun testClient(
    handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData
): ProviderRegistryClient = HttpProviderRegistryClient(
    providerRegistryBaseUrl = "http://localhost",
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

fun createProvider(
    behandlerRef: Uuid,
    dialogmeldingEnabled: Boolean = true,
    dialogmeldingEnabledLocked: Boolean = false,
    kontornavn: String? = null,
    personident: Personident = personident("13326920147"),
    herId: Int? = 654321,
    hprId: Int = 7654321,
    kategori: ProviderCategory = ProviderCategory.DOCTOR,
    orgnummer: String? = "987654321"
) = Provider(
    providerReference = behandlerRef,
    office = ProviderOffice(
        herId = 54321,
        name = kontornavn,
        address = "Storgata 15",
        postalCode = "0158",
        city = "Oslo",
        organisationNumber = orgnummer?.let { organisationNumber(it) },
        dialogMessageEnabled = dialogmeldingEnabled,
        dialogMessageEnabledLocked = dialogmeldingEnabledLocked,
        system = null,
        receivedAt = OffsetDateTime.now()
    ),
    nationalIdentityNumber = personident,
    firstName = "Kari",
    middleName = "Anne",
    lastName = "Hansen",
    herId = herId,
    hprId = hprId,
    phoneNumber = null,
    category = kategori,
    receivedAt = OffsetDateTime.now(),
    suspended = false
)

private fun personident(value: String): Personident =
    Personident(value).getOrElse { error(it.message) }

private fun organisationNumber(value: String): OrganisationNumber =
    OrganisationNumber(value).getOrElse { error(it.message) }
