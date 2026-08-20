package no.nav.helsemelding.outbound.processing.conversion

import no.nav.helsemelding.messageconverter.error.ConversionError
import no.nav.helsemelding.outbound.processing.client.ClientError
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ErrorCode.CONVERSION_ERROR

sealed interface OutgoingMessageError {
    val message: String
    val code: ErrorCode

    data class Conversion(
        val error: ConversionError
    ) : OutgoingMessageError {
        override val message = error.message
        override val code = CONVERSION_ERROR
    }

    data class Client(
        val error: ClientError
    ) : OutgoingMessageError {
        override val message = error.message
        override val code = error.code
    }
}
