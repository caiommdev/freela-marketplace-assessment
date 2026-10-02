package br.com.freela.notificacao.infrastructure.consumer;

import br.com.freela.notificacao.infrastructure.persistence.FailedEvent;
import br.com.freela.notificacao.infrastructure.persistence.FailedEventRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class FailedEventRecoverer implements ConsumerRecordRecoverer {
    private static final String DLT_TOPIC = "notificacao-service.DLT";

    private final FailedEventRepository repository;
    private final KafkaTemplate<Object, Object> template;
    private DeadLetterPublishingRecoverer dltRecoverer;

    @PostConstruct
    void init() {
        this.dltRecoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(DLT_TOPIC, -1));
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        String eventId = header(record, "eventId");
        String eventType = header(record, "eventType");
        String correlationId = header(record, "correlationId");
        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;

        MDC.put("correlationId", correlationId);
        try {
            repository.save(new FailedEvent(
                    eventId, eventType, correlationId,
                    record.topic(), record.partition(), record.offset(),
                    String.valueOf(record.key()), String.valueOf(record.value()),
                    cause.getClass().getName(), cause.getMessage(), stacktrace(cause)));

            log.error("notificacao.evento.dlt eventId={} eventType={} topic={} partition={} offset={} erro={} mensagem={} dlt={}",
                    eventId, eventType, record.topic(), record.partition(), record.offset(),
                    cause.getClass().getSimpleName(), cause.getMessage(), DLT_TOPIC);
        } finally {
            MDC.clear();
        }

        dltRecoverer.accept(record, exception);
    }

    private static String header(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static String stacktrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
