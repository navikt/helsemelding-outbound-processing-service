package no.nav.helsemelding.outbound.processing.conversion

import arrow.core.Either
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.messageconverter.AdditionalMessageInfoProvider
import no.nav.helsemelding.messageconverter.error.ConversionError
import no.nav.helsemelding.messageconverter.msghead.model.AdditionalMessageInfo

class ResolvedAdditionalMessageInfoProvider(
    private val additionalMessageInfo: AdditionalMessageInfo
) : AdditionalMessageInfoProvider {
    override fun getAdditionalMessageInfo(
        dialogMessage: OutgoingDialogMessage
    ): Either<ConversionError, AdditionalMessageInfo> = Either.Right(additionalMessageInfo)
}
