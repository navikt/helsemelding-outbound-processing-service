package no.nav.helsemelding.outbound.processing.conversion

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.withError
import no.nav.helsemelding.messageconverter.AdditionalMessageInfoProvider
import no.nav.helsemelding.messageconverter.MessageConverter
import no.nav.helsemelding.messageconverter.MsgHeadMessageConverter
import no.nav.helsemelding.messageconverter.json.OutgoingDialogMessageSerializer

interface OutgoingMessageConverter {
    suspend fun outgoingDialogMessageJsonToXml(json: String): Either<OutgoingMessageError, String>
}

class MsgHeadOutgoingMessageConverter(
    private val additionalMessageInfoResolver: AdditionalMessageInfoResolver,
    private val outgoingDialogMessageSerializer: OutgoingDialogMessageSerializer = OutgoingDialogMessageSerializer(),
    private val converterFactory: (AdditionalMessageInfoProvider) -> MessageConverter = { provider ->
        MsgHeadMessageConverter(additionalMessageInfoProvider = provider)
    }
) : OutgoingMessageConverter {
    override suspend fun outgoingDialogMessageJsonToXml(json: String): Either<OutgoingMessageError, String> =
        either {
            val dialogMessage = withError(OutgoingMessageError::Conversion) {
                outgoingDialogMessageSerializer.deserialize(json).bind()
            }
            val additionalMessageInfo = additionalMessageInfoResolver.resolve(dialogMessage).bind()

            withError(OutgoingMessageError::Conversion) {
                converterFactory(ResolvedAdditionalMessageInfoProvider(additionalMessageInfo))
                    .outgoingDialogMessageJsonToXml(json)
                    .bind()
            }
        }
}

class FakeOutgoingMessageConverter(
    private val result: Either<OutgoingMessageError, String> = Either.Right("<xml />")
) : OutgoingMessageConverter {
    val payloads = mutableListOf<String>()

    override suspend fun outgoingDialogMessageJsonToXml(json: String): Either<OutgoingMessageError, String> {
        payloads.add(json)
        return result
    }
}
