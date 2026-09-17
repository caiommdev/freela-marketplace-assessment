package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;

import java.time.Instant;
import java.util.UUID;

public record EntregaRegistrada(
        UUID eventId,
        Instant occurredAt,
        UUID contratoId,
        UUID clientId,
        UUID freelancerId
) implements DomainEvent {
    public static EntregaRegistrada novo(Contrato contrato) {
        return new EntregaRegistrada(
                UUID.randomUUID(),
                Instant.now(),
                contrato.id(),
                contrato.clienteId(),
                contrato.freelancerId()
        );
    }

    @Override
    public String eventType() {
        return "EntregaRegistrada";
    }
}
