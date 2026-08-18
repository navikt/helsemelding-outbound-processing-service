package no.nav.helsemelding.outbound.processing.client.providerregistry

import arrow.core.getOrElse
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
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

        val client = testClient { request ->
            request.method shouldBe HttpMethod.Get
            request.url.fullPath shouldBe "/api/v1/behandler/$providerId"

            respond(
                content = externalProviderJson(providerId),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getProvider(providerId)

        val provider = response.shouldBeRight()
        provider.providerReference shouldBe providerId
        provider.nationalIdentityNumber shouldBe personident("13326920147")
        provider.firstName shouldBe "Kari"
        provider.middleName shouldBe "Anne"
        provider.lastName shouldBe "Hansen"
        provider.herId shouldBe null
        provider.hprId shouldBe 7654321
        provider.phoneNumber shouldBe "22000000"
        provider.category shouldBe ProviderCategory.DOCTOR
        provider.office.herId shouldBe null
        provider.office.name shouldBe "Sentrum legesenter"
        provider.office.address shouldBe "Storgata 15"
        provider.office.postalCode shouldBe "0158"
        provider.office.city shouldBe "Oslo"
        provider.office.organisationNumber shouldBe organisationNumber("987654321")
    }

    "status OK should ignore invalid optional identifiers" {
        val providerId = Uuid.random()

        val client = testClient {
            respond(
                content = externalProviderJson(
                    behandlerRef = providerId,
                    fnr = "invalid-fnr",
                    orgnummer = "invalid-orgnummer"
                ),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            )
        }

        val response = client.getProvider(providerId)

        val provider = response.shouldBeRight()
        provider.nationalIdentityNumber shouldBe null
        provider.office.organisationNumber shouldBe null
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
})

private fun externalProviderJson(
    behandlerRef: Uuid,
    fnr: String? = "13326920147",
    orgnummer: String? = "987654321",
    extraField: String = ""
): String =
    externalProviderJson(
        behandlerRef = behandlerRef.toString(),
        fnr = fnr,
        orgnummer = orgnummer,
        extraField = extraField
    )

private fun externalProviderJson(
    behandlerRef: String,
    fnr: String? = "13326920147",
    orgnummer: String? = "987654321",
    extraField: String = ""
): String =
    """
    {
      "type": null,
      "behandlerRef": "$behandlerRef",
      "kategori": "LEGE",
      "fnr": ${fnr.jsonValue()},
      "hprId": 7654321,
      "fornavn": "Kari",
      "mellomnavn": "Anne",
      "etternavn": "Hansen",
      "orgnummer": ${orgnummer.jsonValue()},
      "kontor": "Sentrum legesenter",
      "adresse": "Storgata 15",
      "postnummer": "0158",
      "poststed": "Oslo",
      "telefon": "22000000"
      $extraField
    }
    """.trimIndent()

private fun String?.jsonValue(): String =
    this?.let { """"$it"""" } ?: "null"

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
