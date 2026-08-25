package no.nav.helsemelding.outbound.processing.conversion

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensureNotNull
import arrow.core.raise.withError
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.messageconverter.error.AdditionalMessageInfoError
import no.nav.helsemelding.messageconverter.msghead.model.AdditionalMessageInfo
import no.nav.helsemelding.messageconverter.msghead.model.Employee
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.messageconverter.msghead.model.provider.Provider
import no.nav.helsemelding.outbound.processing.client.pdl.PdlClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.ProviderRegistryClient
import no.nav.helsemelding.outbound.processing.config.ProviderHerIdOverride
import kotlin.uuid.Uuid

interface AdditionalMessageInfoResolver {
    suspend fun resolve(dialogMessage: OutgoingDialogMessage): Either<OutgoingMessageError, AdditionalMessageInfo>
}

class HttpAdditionalMessageInfoResolver(
    private val pdlClient: PdlClient,
    private val providerRegistryClient: ProviderRegistryClient,
    private val providerHerIdOverride: ProviderHerIdOverride
) : AdditionalMessageInfoResolver {
    override suspend fun resolve(dialogMessage: OutgoingDialogMessage): Either<OutgoingMessageError, AdditionalMessageInfo> =
        either {
            val providerId = dialogMessage.providerUuid().bind()
            val provider = withError(OutgoingMessageError::Client) {
                providerRegistryClient.getProvider(providerId).bind()
            }
                .let(providerHerIdOverride::apply)

            val patientIdent = withError(OutgoingMessageError::Conversion) {
                Personident(dialogMessage.patientIdent).bind()
            }
            val employee = withError(OutgoingMessageError::Client) {
                pdlClient.getPersonName(patientIdent)
                    .map {
                        Employee(
                            personident = patientIdent,
                            firstName = it.firstName,
                            middleName = it.middleName,
                            lastName = it.lastName
                        )
                    }
                    .bind()
            }

            AdditionalMessageInfo(provider, employee)
        }
}

private fun ProviderHerIdOverride.apply(provider: Provider): Provider =
    provider.copy(
        herId = herId ?: provider.herId,
        office = provider.office.copy(
            herId = officeHerId ?: provider.office.herId
        )
    )

private fun OutgoingDialogMessage.providerUuid(): Either<OutgoingMessageError, Uuid> =
    either {
        ensureNotNull(Uuid.parseOrNull(providerId)) {
            OutgoingMessageError.Conversion(
                AdditionalMessageInfoError("Invalid providerId: $providerId")
            )
        }
    }

class FakeAdditionalMessageInfoResolver : AdditionalMessageInfoResolver {
    private val additionalMessageInfoByMessageId =
        mutableMapOf<String, Either<OutgoingMessageError, AdditionalMessageInfo>>()

    fun givenAdditionalMessageInfo(
        messageId: String,
        either: Either<OutgoingMessageError, AdditionalMessageInfo>
    ) {
        additionalMessageInfoByMessageId[messageId] = either
    }

    override suspend fun resolve(
        dialogMessage: OutgoingDialogMessage
    ): Either<OutgoingMessageError, AdditionalMessageInfo> =
        additionalMessageInfoByMessageId[dialogMessage.id]
            ?: Either.Left(
                OutgoingMessageError.Conversion(
                    AdditionalMessageInfoError("Missing additional message info for message ${dialogMessage.id}")
                )
            )
}
