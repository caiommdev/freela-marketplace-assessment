package br.com.freela.contrato.infrastructure.event.outbox;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Getter
public class OutboxEvent {
    @Id
    private UUID id;

    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false, updatable = false, insertable = false)
    private Long seq;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String topic;

    @Column(columnDefinition = "text", nullable = false)
    private String payload;

    @Column(nullable = false)
    private String correlationId;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant sentAt;

    protected OutboxEvent() {}

    public OutboxEvent(UUID id, UUID aggregateId, String eventType,
                       String topic, String payload, String correlationId) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.payload = payload;
        this.correlationId = correlationId;
        this.createdAt = Instant.now();
    }

    public void marcarEnviado() {
        this.sentAt = Instant.now();
    }
}
