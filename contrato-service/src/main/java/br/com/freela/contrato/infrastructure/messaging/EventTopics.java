package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.event.ContratoCriado;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EventTopics {
    private final Map<Class<?>, String> topicsByEventType;

    public EventTopics(
            @Value("${spring.kafka.topics.freela-marketplace.contrato.criado}") String contratoCriadoTopic
    ) {
        this.topicsByEventType = Map.of(
                ContratoCriado.class, contratoCriadoTopic
        );
    }

    public String getTopic(Class<?> eventClass) {
        String topic = topicsByEventType.get(eventClass);
        if (topic == null) {
            throw new IllegalArgumentException("Nenhum tópico Kafka configurado para o evento: " + eventClass.getName());
        }
        return topic;
    }
}
