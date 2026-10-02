package br.com.freela.notificacao.infrastructure.persistence;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "failed_events")
public class FailedEvent {
    @Id
    public UUID id;

    public String eventId;
    public String eventType;
    public String correlationId;

    public String topic;

    @Column(name = "partition_num")
    public Integer partition;

    @Column(name = "kafka_offset")
    public Long offset;

    public String messageKey;

    @Column(columnDefinition = "text")
    public String payload;

    public String errorClass;

    @Column(columnDefinition = "text")
    public String errorMessage;

    @Column(columnDefinition = "text")
    public String stacktrace;

    public Instant failedAt;

    public boolean reprocessed;

    protected FailedEvent() {}

    public FailedEvent(String eventId, String eventType, String correlationId,
                       String topic, int partition, long offset, String messageKey,
                       String payload, String errorClass, String errorMessage, String stacktrace) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.eventType = eventType;
        this.correlationId = correlationId;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.messageKey = messageKey;
        this.payload = payload;
        this.errorClass = errorClass;
        this.errorMessage = errorMessage;
        this.stacktrace = stacktrace;
        this.failedAt = Instant.now();
        this.reprocessed = false;
    }
}
