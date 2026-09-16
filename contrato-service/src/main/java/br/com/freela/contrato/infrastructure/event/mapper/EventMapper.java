package br.com.freela.contrato.infrastructure.event.mapper;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.event.kafka.KafkaEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class EventMapper {
    private final Map<Class<? extends DomainEvent>,
                DomainEventMapper<DomainEvent, KafkaEvent>> mappers;

    @SuppressWarnings("unchecked")
    public EventMapper(
            List<DomainEventMapper<? extends DomainEvent, ? extends KafkaEvent>> mappersList) {
        this.mappers = mappersList
                .stream()
                .collect(
                    Collectors.toMap(
                            DomainEventMapper::domainEventType,
                            mapper -> (DomainEventMapper<DomainEvent, KafkaEvent>) mapper
                    )
                );
    }

    public KafkaEvent toKafkaEvent(DomainEvent domainEvent) {
        DomainEventMapper<DomainEvent, KafkaEvent> mapper = mappers.get(domainEvent.getClass());
        if (mapper == null) {
            throw new IllegalArgumentException(
                    "Nenhum mapper Kafka configurado para o evento: " + domainEvent.getClass().getName()
            );
        }
        return mapper.toKafkaEvent(domainEvent);
    }
}
