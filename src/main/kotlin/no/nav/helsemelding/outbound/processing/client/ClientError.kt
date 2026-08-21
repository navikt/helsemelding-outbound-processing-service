package no.nav.helsemelding.outbound.processing.client

import no.nav.helsemelding.outbound.processing.model.ErrorCode

interface ClientError {
    val message: String
    val code: ErrorCode
}
