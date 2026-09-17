package br.com.freela.contrato.infrastructure.event.mapper;

import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.infrastructure.event.kafka.EntregaRegistradaKafkaEvent;
import org.springframework.stereotype.Component;

@Component
public class EntregaRegistradaMapper
        implements DomainEventMapper<EntregaRegistrada, EntregaRegistradaKafkaEvent>{
    @Override
    public Class<EntregaRegistrada> domainEventType() {
        return EntregaRegistrada.class;
    }

    @Override
    public EntregaRegistradaKafkaEvent toKafkaEvent(EntregaRegistrada domainEvent) {
        return new EntregaRegistradaKafkaEvent(
                domainEvent.eventId().toString(),
                domainEvent.occurredAt().toString(),
                domainEvent.contratoId(),
                domainEvent.clientId().toString(),
                domainEvent.freelancerId().toString()
        );
    }
}
