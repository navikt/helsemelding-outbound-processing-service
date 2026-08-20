package no.nav.helsemelding.outbound.processing.client.providerregistry.model

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.Serializable
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.messageconverter.msghead.model.provider.OrganisationNumber
import no.nav.helsemelding.messageconverter.msghead.model.provider.Provider
import no.nav.helsemelding.messageconverter.msghead.model.provider.ProviderCategory
import no.nav.helsemelding.messageconverter.msghead.model.provider.ProviderOffice
import java.time.OffsetDateTime
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

@Serializable
data class ExternalProvider(
    val type: String? = null,
    val behandlerRef: String,
    val kategori: ProviderCategory,
    val fnr: String? = null,
    val hprId: Int? = null,
    val herId: Int? = null,
    val fornavn: String,
    val mellomnavn: String? = null,
    val etternavn: String,
    val orgnummer: String? = null,
    val kontor: String? = null,
    val kontorHerId: Int? = null,
    val adresse: String? = null,
    val postnummer: String? = null,
    val poststed: String? = null,
    val telefon: String? = null
)

fun ExternalProvider.toProvider(): Provider =
    Provider(
        providerReference = Uuid.parse(behandlerRef),
        nationalIdentityNumber = fnr?.let(::parsePersonident),
        firstName = fornavn,
        middleName = mellomnavn,
        lastName = etternavn,
        herId = herId,
        hprId = hprId,
        phoneNumber = telefon,
        office = ProviderOffice(
            herId = kontorHerId,
            name = kontor,
            address = adresse,
            postalCode = postnummer,
            city = poststed,
            organisationNumber = orgnummer?.let(::parseOrganisationNumber),
            dialogMessageEnabled = true,
            dialogMessageEnabledLocked = false,
            system = null,
            receivedAt = OffsetDateTime.now()
        ),
        category = kategori,
        receivedAt = OffsetDateTime.now(),
        suspended = false
    )

private fun parsePersonident(value: String): Personident? =
    Personident(value).getOrNull().also {
        if (it == null) {
            log.error { "Invalid national identity number from ProviderRegistry response" }
        }
    }

private fun parseOrganisationNumber(value: String): OrganisationNumber? =
    OrganisationNumber(value).getOrNull().also {
        if (it == null) {
            log.error { "Invalid organisation number from ProviderRegistry response" }
        }
    }
