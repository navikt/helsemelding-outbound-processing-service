package no.nav.helsemelding.outbound.processing.conversion

import arrow.core.Either
import arrow.core.raise.either
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.messageconverter.error.AdditionalMessageInfoError
import no.nav.helsemelding.messageconverter.error.ConversionError
import no.nav.helsemelding.messageconverter.msghead.model.AdditionalMessageInfo
import no.nav.helsemelding.messageconverter.msghead.model.Employee
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.PdlClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.ProviderRegistryClient
import kotlin.uuid.Uuid

interface AdditionalMessageInfoResolver {
    suspend fun resolve(dialogMessage: OutgoingDialogMessage): Either<ConversionError, AdditionalMessageInfo>
}

class HttpAdditionalMessageInfoResolver(
    private val pdlClient: PdlClient,
    private val providerRegistryClient: ProviderRegistryClient
) : AdditionalMessageInfoResolver {
    override suspend fun resolve(dialogMessage: OutgoingDialogMessage): Either<ConversionError, AdditionalMessageInfo> =
        either {
            val providerId = dialogMessage.providerUuid().bind()
            val provider = providerRegistryClient.getProvider(providerId)
                .mapLeft { AdditionalMessageInfoError(it.message) }
                .bind()

            val patientIdent = Personident(dialogMessage.patientIdent).bind()
            val employee = pdlClient.getPersonName(patientIdent)
                .mapLeft { AdditionalMessageInfoError(it.message) }
                .map {
                    Employee(
                        personident = patientIdent,
                        firstName = it.firstName,
                        middleName = it.middleName,
                        lastName = it.lastName
                    )
                }
                .bind()

            AdditionalMessageInfo(provider, employee)
        }
}

private fun OutgoingDialogMessage.providerUuid(): Either<ConversionError, Uuid> =
    Either.catch { Uuid.parse(providerId) }
        .mapLeft { AdditionalMessageInfoError("Invalid providerId: $providerId", it) }

class FakeAdditionalMessageInfoResolver : AdditionalMessageInfoResolver {
    private val additionalMessageInfoByMessageId =
        mutableMapOf<String, Either<ConversionError, AdditionalMessageInfo>>()

    fun givenAdditionalMessageInfo(
        messageId: String,
        either: Either<ConversionError, AdditionalMessageInfo>
    ) {
        additionalMessageInfoByMessageId[messageId] = either
    }

    override suspend fun resolve(
        dialogMessage: OutgoingDialogMessage
    ): Either<ConversionError, AdditionalMessageInfo> =
        additionalMessageInfoByMessageId[dialogMessage.id]
            ?: Either.Left(AdditionalMessageInfoError("Missing additional message info for message ${dialogMessage.id}"))
}
