package no.nav.helsemelding.outbound.processing.client.providerregistry

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
import no.nav.helsemelding.message.msghead.model.Personident
import no.nav.helsemelding.message.msghead.model.provider.OrganisationNumber
import no.nav.helsemelding.message.msghead.model.provider.Provider
import no.nav.helsemelding.message.msghead.model.provider.ProviderCategori
import no.nav.helsemelding.message.msghead.model.provider.ProviderOffice
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.FetchingError
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

    "status not OK should return FetchingError" {
        val client = testClient {
            respond(
                content = "Provider not found",
                status = HttpStatusCode.NotFound
            )
        }

        val response = client.getProvider(Uuid.random())

        val error = response.shouldBeLeft()
        val fetchingError = error.shouldBeInstanceOf<FetchingError>()
        fetchingError.code shouldBe HttpStatusCode.NotFound.value
        fetchingError.message shouldBe "Provider not found"
    }
})

private fun testClient(
    handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData
): ProviderRegistryClient = HttpProviderRegistryClient(
    providerRegistryServiceUrl = "http://localhost",
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
    personident: Personident = Personident("13326920147"),
    herId: Int? = 654321,
    hprId: Int = 7654321,
    kategori: ProviderCategori = ProviderCategori.LEGE,
    orgnummer: String? = "987654321"
) = Provider(
    behandlerRef = behandlerRef,
    kontor = ProviderOffice(
        herId = 54321,
        navn = kontornavn,
        adresse = "Storgata 15",
        postnummer = "0158",
        poststed = "Oslo",
        orgnummer = orgnummer?.let { OrganisationNumber(it) },
        dialogmeldingEnabled = dialogmeldingEnabled,
        dialogmeldingEnabledLocked = dialogmeldingEnabledLocked,
        system = null,
        mottatt = OffsetDateTime.now()
    ),
    personident = personident,
    fornavn = "Kari",
    mellomnavn = "Anne",
    etternavn = "Hansen",
    herId = herId,
    hprId = hprId,
    telefon = null,
    kategori = kategori,
    mottatt = OffsetDateTime.now(),
    suspendert = false
)
