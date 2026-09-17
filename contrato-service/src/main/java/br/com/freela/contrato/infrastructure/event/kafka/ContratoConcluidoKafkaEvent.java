package br.com.freela.contrato.infrastructure.event.kafka;

import java.util.UUID;

public record ContratoConcluidoKafkaEvent(
    String eventId,
    String occurredAt,
    UUID contratoId,
    String clienteId,
    String freelancerId,
    String valor
) implements KafkaEvent {

    @Override
    public UUID contractId() {
        return contratoId;
    }
}
