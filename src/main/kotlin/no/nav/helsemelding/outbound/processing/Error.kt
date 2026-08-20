package no.nav.helsemelding.outbound.processing

sealed interface Error

sealed interface PublishError : Error {
    val key: String
    val topic: String
    val cause: Throwable

    data class Failure(
        override val key: String,
        override val topic: String,
        override val cause: Throwable
    ) : PublishError
}
