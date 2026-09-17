package br.com.freela.contrato.infrastructure.event.kafka;

import java.util.UUID;

public interface KafkaEvent {
    UUID contractId();
}
