package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.DomainEventPublisher;
import br.com.freela.contrato.infrastructure.event.kafka.KafkaEvent;
import br.com.freela.contrato.infrastructure.event.mapper.EventMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements DomainEventPublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaEventTopics topics;
    private final EventMapper mapper;

    @Override
    public void publish(DomainEvent event) {
        KafkaEvent kafkaEvent = mapper.toKafkaEvent(event);
        kafkaTemplate.send(
                topics.getTopic(event.getClass()), kafkaEvent.contractId(), kafkaEvent);
    }

}
