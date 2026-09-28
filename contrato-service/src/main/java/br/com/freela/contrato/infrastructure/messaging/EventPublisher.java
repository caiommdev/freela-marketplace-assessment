package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.DomainEventPublisher;
import br.com.freela.contrato.infrastructure.event.kafka.KafkaEvent;
import br.com.freela.contrato.infrastructure.event.mapper.EventMapper;
import br.com.freela.contrato.infrastructure.event.outbox.OutboxEvent;
import br.com.freela.contrato.infrastructure.persistence.outbox.OutboxRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EventPublisher implements DomainEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventTopics topics;
    private final EventMapper mapper;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Propagator propagator;
    private final Tracer tracer;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(DomainEvent event)
     {
        KafkaEvent kafkaEvent = mapper.toKafkaEvent(event);
        String topic = topics.getTopic(event.getClass());
        String correlationId = resolveCorrelationId();

         String payload;
         String tracerContext;
         try {
             payload = objectMapper.writeValueAsString(kafkaEvent);

             Map<String, String> carrier = new HashMap<>();
             TraceContext context = tracer.currentTraceContext().context();
             if (context != null) {
                 propagator.inject(context, carrier, Map::put);
             }
             tracerContext = objectMapper.writeValueAsString(carrier);
         } catch (Exception e) {
             throw new IllegalStateException("Falha ao serializar evento " + event.eventType(), e);
         }

        var outbox = new OutboxEvent(
                event.eventId(),
                kafkaEvent.contractId(),
                event.eventType(),
                topic,
                payload,
                correlationId,
                tracerContext
        );
        outboxRepository.save(outbox);
    }

    public void publishOutbox(OutboxEvent event, String topic, String correlationId) {
        Map<String, String> carrier;
        try {
            carrier = objectMapper.readValue(
                    event.getTraceContext(), new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao desserializar traceContext do evento " + event.getId(), e);
        }
        Span span = propagator.extract(carrier, Map::get)
                .name("outbox-publish " + event.getEventType())
                .start();
        try (var ws = tracer.withSpan(span)) {
            log.info("outbox.evento.gravado eventId={} eventType={} contratoId={} topic={} correlationId={}",
                    event.getId(), event.getEventType(), event.getAggregateId(), topic, correlationId);

            var record = new ProducerRecord<>(
                    topic,
                    event.getAggregateId().toString(),
                    event.getPayload());
            record.headers().add("eventId", event.getId().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));
            record.headers().add("correlationId", event.getCorrelationId().getBytes(StandardCharsets.UTF_8));

            propagator.inject(span.context(), record.headers(),
                    (headers, key, value) -> headers.add(key, value.getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).join();

            event.marcarEnviado();
        } finally {
            span.end();
        }
    }

    private String resolveCorrelationId() {
        String cid = MDC.get("correlationId");
        return (cid == null || cid.isBlank()) ? UUID.randomUUID().toString() : cid;
    }
}
