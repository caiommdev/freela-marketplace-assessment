package br.com.freela.contrato.infrastructure.event.mapper;

import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.infrastructure.event.kafka.ContratoConcluidoKafkaEvent;
import org.springframework.stereotype.Component;

@Component
public class ContratoConcluidoMapper
        implements DomainEventMapper<ContratoConcluido, ContratoConcluidoKafkaEvent>{
    @Override
    public Class<ContratoConcluido> domainEventType() {
        return ContratoConcluido.class;
    }

    @Override
    public ContratoConcluidoKafkaEvent toKafkaEvent(ContratoConcluido domainEvent) {
        return new ContratoConcluidoKafkaEvent(
                domainEvent.eventId().toString(),
                domainEvent.occurredAt().toString(),
                domainEvent.contratoId(),
                domainEvent.clienteId().toString(),
                domainEvent.freelancerId().toString(),
                domainEvent.valor().toString()
        );
    }
}
