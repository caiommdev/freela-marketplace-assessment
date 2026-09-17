package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(
        UUID eventId,
        Instant occurredAt,
        UUID contratoId,
        UUID clienteId,
        UUID freelancerId
) implements DomainEvent {

    public static ContratoCancelado novo(Contrato contrato) {
        return new ContratoCancelado(
                UUID.randomUUID(),
                Instant.now(),
                contrato.id(),
                contrato.clienteId(),
                contrato.freelancerId());
    }
    @Override public String eventType() {
        return "ContratoCancelado";
    }
}