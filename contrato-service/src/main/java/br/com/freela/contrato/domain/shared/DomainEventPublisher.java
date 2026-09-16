package br.com.freela.contrato.domain.shared;

import java.util.Collection;

public interface DomainEventPublisher {
    void publish(DomainEvent event);
}
