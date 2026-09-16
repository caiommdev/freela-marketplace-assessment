package br.com.freela.contrato.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;

public record ContratoCriado
        (
            UUID eventId,
            Instant occurredAt,
            UUID contratoId,
            UUID clienteId,
            UUID freelancerId,
            String titulo,
            BigDecimal valor
        ) implements DomainEvent {

    public static ContratoCriado novo(Contrato contrato) {
        return new ContratoCriado(
                UUID.randomUUID(),
                Instant.now(),
                contrato.id(),
                contrato.clienteId(),
                contrato.freelancerId(),
                contrato.titulo(),
                contrato.valor()
        );
    }
    @Override public  String eventType() {
        return "ContratoCriado";
    }
}
