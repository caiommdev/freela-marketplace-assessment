package br.com.freela.contrato.infrastructure.event.mapper;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.event.kafka.KafkaEvent;

public interface DomainEventMapper<D extends DomainEvent, K extends KafkaEvent> {
    Class<D> domainEventType();
    K toKafkaEvent(D domainEvent);
}
