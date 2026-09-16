package br.com.freela.contrato.infrastructure.event.kafka;

public record ContratoCriadoKafkaEvent(
    String eventId,
    String occurredAt,
    String contratoId,
    String clienteId,
    String freelancerId,
    String titulo,
    String valor
) implements KafkaEvent {

    @Override
    public String contractId() {
        return contratoId;
    }
}
