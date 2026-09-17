package br.com.freela.contrato.infrastructure.event.kafka;

import java.util.UUID;

public record ContratoCanceladoKafkaEvent(
    String eventId,
    String occurredAt,
    UUID contratoId,
    String clienteId,
    String freelancerId
) implements KafkaEvent {

    @Override
    public UUID contractId() {
        return contratoId;
    }
}
