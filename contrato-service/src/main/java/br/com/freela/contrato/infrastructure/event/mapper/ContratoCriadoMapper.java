package br.com.freela.contrato.infrastructure.event.mapper;

import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.infrastructure.event.kafka.ContratoCriadoKafkaEvent;
import org.springframework.stereotype.Component;

@Component
public class ContratoCriadoMapper
        implements DomainEventMapper<ContratoCriado, ContratoCriadoKafkaEvent>{
    @Override
    public Class<ContratoCriado> domainEventType() {
        return ContratoCriado.class;
    }

    @Override
    public ContratoCriadoKafkaEvent toKafkaEvent(ContratoCriado domainEvent) {
        return new ContratoCriadoKafkaEvent(
                domainEvent.eventId().toString(),
                domainEvent.occurredAt().toString(),
                domainEvent.contratoId(),
                domainEvent.clienteId().toString(),
                domainEvent.freelancerId().toString(),
                domainEvent.titulo(),
                domainEvent.valor().toString()
        );
    }
}
