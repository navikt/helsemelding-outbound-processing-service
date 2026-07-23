package no.nav.helsemelding.outbound.processing.client.additionalInfo

import arrow.core.Either
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equality.shouldBeEqualUsingFields
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import no.nav.helsemelding.jsonschema.core.model.ConversationReference
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessageType
import no.nav.helsemelding.message.error.AdditionalMessageInfoError
import no.nav.helsemelding.message.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.FakePdlClient
import no.nav.helsemelding.outbound.processing.client.pdl.PdlClient
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlPersonNavn
import no.nav.helsemelding.outbound.processing.client.providerregistry.FakeProviderRegistryClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.ProviderRegistryClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.createProvider
import kotlin.uuid.Uuid

class HttpAdditionalMessageInfoProviderSpec : StringSpec({

    val providerId = Uuid.random()
    val patientIdent = Personident("24274116206")
    val dialogMessage = OutgoingDialogMessage(
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

    "should return additional message info if no errors" {
        val providerRegistryClient = FakeProviderRegistryClient()
        val provider = createProvider(providerId)
        providerRegistryClient.givenProvider(providerId, Either.Right(provider))
        val pdlClient = FakePdlClient()
        val personName = PdlPersonNavn(
            fornavn = "Ola",
            mellomnavn = "Jens",
            etternavn = "Nordmann"
        )
        pdlClient.givenPersonName(patientIdent, Either.Right(personName))

        val infoProvider = additionalMessageInfoProvider(pdlClient, providerRegistryClient)

        val info = infoProvider.getAdditionalMessageInfo(dialogMessage).shouldBeRight()
        info.employee.personident shouldBe patientIdent
        info.employee.firstName shouldBe personName.fornavn
        info.employee.middleName shouldBe personName.mellomnavn
        info.employee.lastName shouldBe personName.etternavn
        info.provider shouldBeEqualUsingFields provider
    }

    "should return AdditionalMessageInfoError when provider registry returns error" {
        val providerRegistryClient = FakeProviderRegistryClient()
        val provider = additionalMessageInfoProvider(
            providerRegistryClient = providerRegistryClient
        )
        val error = provider.getAdditionalMessageInfo(dialogMessage).shouldBeLeft()

        error.shouldBeInstanceOf<AdditionalMessageInfoError>()
        error.message shouldBe "Error when fetching provider"
    }

    "should return AdditionalMessageInfoError when pdl returns error" {
        val providerRegistryClient = FakeProviderRegistryClient()
        val provider = createProvider(providerId)
        providerRegistryClient.givenProvider(provider.behandlerRef, Either.Right(provider))
        val infoProvider = additionalMessageInfoProvider(
            providerRegistryClient = providerRegistryClient
        )

        val error = infoProvider.getAdditionalMessageInfo(dialogMessage).shouldBeLeft()

        error.shouldBeInstanceOf<AdditionalMessageInfoError>()
        error.message shouldBe "Error when fetching person name"
    }
})

private fun additionalMessageInfoProvider(
    pdlClient: PdlClient = FakePdlClient(),
    providerRegistryClient: ProviderRegistryClient = FakeProviderRegistryClient()
): AdditionalMessageInfoProvider = HttpAdditionalMessageInfoProvider(
    pdlClient = pdlClient,
    providerRegistryClient = providerRegistryClient
)
