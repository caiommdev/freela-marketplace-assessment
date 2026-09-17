package br.com.freela.contrato.infrastructure.event.mapper;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.infrastructure.event.kafka.ContratoCanceladoKafkaEvent;
import org.springframework.stereotype.Component;

@Component
public class ContratoCanceladoMapper
        implements DomainEventMapper<ContratoCancelado, ContratoCanceladoKafkaEvent>{
    @Override
    public Class<ContratoCancelado> domainEventType() {
        return ContratoCancelado.class;
    }

    @Override
    public ContratoCanceladoKafkaEvent toKafkaEvent(ContratoCancelado domainEvent) {
        return new ContratoCanceladoKafkaEvent(
                domainEvent.eventId().toString(),
                domainEvent.occurredAt().toString(),
                domainEvent.contratoId(),
                domainEvent.clienteId().toString(),
                domainEvent.freelancerId().toString()
        );
    }
}
