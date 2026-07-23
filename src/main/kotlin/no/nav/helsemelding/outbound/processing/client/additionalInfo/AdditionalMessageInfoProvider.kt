package no.nav.helsemelding.outbound.processing.client.additionalInfo

import arrow.core.Either
import arrow.core.raise.either
import kotlinx.coroutines.runBlocking
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.message.error.AdditionalMessageInfoError
import no.nav.helsemelding.message.error.ConversionError
import no.nav.helsemelding.message.msghead.model.AdditionalMessageInfo
import no.nav.helsemelding.message.msghead.model.Employee
import no.nav.helsemelding.message.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.PdlClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.ProviderRegistryClient
import kotlin.uuid.Uuid

interface AdditionalMessageInfoProvider {
    fun getAdditionalMessageInfo(dialogMessage: OutgoingDialogMessage): Either<ConversionError, AdditionalMessageInfo>
}

class HttpAdditionalMessageInfoProvider(
    private val pdlClient: PdlClient,
    private val providerRegistryClient: ProviderRegistryClient
) : AdditionalMessageInfoProvider {
    override fun getAdditionalMessageInfo(dialogMessage: OutgoingDialogMessage): Either<ConversionError, AdditionalMessageInfo> =
        either {
            val provider = runBlocking {
                providerRegistryClient.getProvider(Uuid.parse(dialogMessage.providerId))
                    .mapLeft { AdditionalMessageInfoError(it.message) }
                    .bind()
            }

            val patientIdent = Personident(dialogMessage.patientIdent)
            val employee = runBlocking {
                pdlClient.getPersonName(patientIdent)
                    .mapLeft { AdditionalMessageInfoError(it.message) }
                    .map {
                        Employee(
                            personident = patientIdent,
                            firstName = it.fornavn,
                            middleName = it.mellomnavn,
                            lastName = it.etternavn
                        )
                    }
                    .bind()
            }

            return Either.Right(AdditionalMessageInfo(provider, employee))
        }
}
