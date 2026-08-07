package no.nav.helsemelding.outbound.processing.conversion

import arrow.core.Either
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.nav.helsemelding.messageconverter.AdditionalMessageInfoProvider
import no.nav.helsemelding.messageconverter.MessageConverter
import no.nav.helsemelding.messageconverter.error.AdditionalMessageInfoError
import no.nav.helsemelding.messageconverter.json.OutgoingDialogMessageSerializer
import no.nav.helsemelding.messageconverter.msghead.model.AdditionalMessageInfo
import no.nav.helsemelding.messageconverter.msghead.model.Employee
import no.nav.helsemelding.outbound.processing.client.providerregistry.createProvider
import kotlin.uuid.Uuid

class OutgoingMessageConverterSpec : StringSpec({

    "should resolve additional info before converting with synchronous provider" {
        val dialogMessage = outgoingDialogMessage()
        val json = OutgoingDialogMessageSerializer().serialize(dialogMessage).shouldBeRight()
        val additionalMessageInfo = additionalMessageInfo()
        val resolver = FakeAdditionalMessageInfoResolver().apply {
            givenAdditionalMessageInfo(dialogMessage.id, Either.Right(additionalMessageInfo))
        }
        val messageConverter = mockk<MessageConverter>()
        lateinit var provider: AdditionalMessageInfoProvider
        every { messageConverter.outgoingDialogMessageJsonToXml(json) } answers {
            provider.getAdditionalMessageInfo(dialogMessage).shouldBeRight() shouldBe additionalMessageInfo
            Either.Right("<xml />")
        }

        val converter = MsgHeadOutgoingMessageConverter(
            additionalMessageInfoResolver = resolver,
            converterFactory = {
                provider = it
                messageConverter
            }
        )

        converter.outgoingDialogMessageJsonToXml(json).shouldBeRight() shouldBe "<xml />"

        verify(exactly = 1) {
            messageConverter.outgoingDialogMessageJsonToXml(json)
        }
    }

    "should return resolver error without converting" {
        val dialogMessage = outgoingDialogMessage()
        val json = OutgoingDialogMessageSerializer().serialize(dialogMessage).shouldBeRight()
        val resolver = FakeAdditionalMessageInfoResolver()
        val messageConverter = mockk<MessageConverter>()

        val converter = MsgHeadOutgoingMessageConverter(
            additionalMessageInfoResolver = resolver,
            converterFactory = { messageConverter }
        )

        val error = converter.outgoingDialogMessageJsonToXml(json).shouldBeLeft()

        error.shouldBeInstanceOf<AdditionalMessageInfoError>()
        error.message shouldBe "Missing additional message info for message ${dialogMessage.id}"
        verify(exactly = 0) {
            messageConverter.outgoingDialogMessageJsonToXml(any())
        }
    }
})

private fun additionalMessageInfo(): AdditionalMessageInfo {
    val personident = personident("24274116206")

    return AdditionalMessageInfo(
        provider = createProvider(Uuid.random()),
        employee = Employee(
            personident = personident,
            firstName = "Ola",
            middleName = "Jens",
            lastName = "Nordmann"
        )
    )
}
