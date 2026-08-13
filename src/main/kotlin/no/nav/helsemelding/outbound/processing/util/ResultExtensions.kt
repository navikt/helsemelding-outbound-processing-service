package no.nav.helsemelding.outbound.processing.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right

fun <L, R> Result<R>.toEither(
    onFailure: (Throwable) -> L
): Either<L, R> =
    fold(
        onSuccess = { it.right() },
        onFailure = { onFailure(it).left() }
    )
