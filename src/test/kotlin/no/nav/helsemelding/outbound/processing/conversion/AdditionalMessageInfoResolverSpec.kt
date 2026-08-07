package no.nav.helsemelding.outbound.processing.conversion

import arrow.core.Either
import arrow.core.getOrElse
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equality.shouldBeEqualUsingFields
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import no.nav.helsemelding.jsonschema.core.model.ConversationReference
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessageType
import no.nav.helsemelding.messageconverter.error.AdditionalMessageInfoError
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.FakePdlClient
import no.nav.helsemelding.outbound.processing.client.pdl.PdlClient
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonName
import no.nav.helsemelding.outbound.processing.client.providerregistry.FakeProviderRegistryClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.ProviderRegistryClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.createProvider
import kotlin.uuid.Uuid

class AdditionalMessageInfoResolverSpec : StringSpec({

    val providerId = Uuid.random()
    val patientIdent = personident("24274116206")
    val dialogMessage = outgoingDialogMessage(
        patientIdent = patientIdent,
        providerId = providerId
    )

    "should resolve additional message info if no errors" {
        val providerRegistryClient = FakeProviderRegistryClient()
        val provider = createProvider(providerId)
        providerRegistryClient.givenProvider(providerId, Either.Right(provider))
        val pdlClient = FakePdlClient()
        val personName = PersonName(
            firstName = "Ola",
            middleName = "Jens",
            lastName = "Nordmann"
        )
        pdlClient.givenPersonName(patientIdent, Either.Right(personName))

        val resolver = additionalMessageInfoResolver(pdlClient, providerRegistryClient)

        val info = resolver.resolve(dialogMessage).shouldBeRight()
        info.employee.personident shouldBe patientIdent
        info.employee.firstName shouldBe personName.firstName
        info.employee.middleName shouldBe personName.middleName
        info.employee.lastName shouldBe personName.lastName
        info.provider shouldBeEqualUsingFields provider
    }

    "should return AdditionalMessageInfoError when provider registry returns error" {
        val providerRegistryClient = FakeProviderRegistryClient()
        val resolver = additionalMessageInfoResolver(
            providerRegistryClient = providerRegistryClient
        )

        val error = resolver.resolve(dialogMessage).shouldBeLeft()

        error.shouldBeInstanceOf<AdditionalMessageInfoError>()
        error.message shouldBe "Error when fetching provider"
    }

    "should return AdditionalMessageInfoError when pdl returns error" {
        val providerRegistryClient = FakeProviderRegistryClient()
        val provider = createProvider(providerId)
        providerRegistryClient.givenProvider(provider.providerReference, Either.Right(provider))
        val resolver = additionalMessageInfoResolver(
            providerRegistryClient = providerRegistryClient
        )

        val error = resolver.resolve(dialogMessage).shouldBeLeft()

        error.shouldBeInstanceOf<AdditionalMessageInfoError>()
        error.message shouldBe "Error when fetching person name"
    }

    "should return AdditionalMessageInfoError when provider id is invalid" {
        val resolver = additionalMessageInfoResolver()
        val dialogMessageWithInvalidProviderId = dialogMessage.copy(providerId = "not-a-uuid")

        val error = resolver.resolve(dialogMessageWithInvalidProviderId).shouldBeLeft()

        error.shouldBeInstanceOf<AdditionalMessageInfoError>()
        error.message shouldBe "Invalid providerId: not-a-uuid"
    }
})

private fun additionalMessageInfoResolver(
    pdlClient: PdlClient = FakePdlClient(),
    providerRegistryClient: ProviderRegistryClient = FakeProviderRegistryClient()
): AdditionalMessageInfoResolver = HttpAdditionalMessageInfoResolver(
    pdlClient = pdlClient,
    providerRegistryClient = providerRegistryClient
)

fun outgoingDialogMessage(
    patientIdent: Personident = personident("24274116206"),
    providerId: Uuid = Uuid.random()
) = OutgoingDialogMessage(
    version = 1,
    id = Uuid.random().toString(),
    patientIdent = patientIdent.value,
    providerId = providerId.toString(),
    conversationReference = ConversationReference(
        parentMessageId = Uuid.random().toString(),
        conversationId = Uuid.random().toString()
    ),
    type = OutgoingDialogMessageType.NAV_MESSAGE,
    message = "Hei",
    attachment = null
)

fun personident(value: String): Personident =
    Personident(value).getOrElse { error(it.message) }
