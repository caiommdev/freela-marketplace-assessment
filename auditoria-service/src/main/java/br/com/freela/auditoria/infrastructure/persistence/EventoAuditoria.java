package br.com.freela.auditoria.infrastructure.persistence;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "auditoria_eventos")
public class EventoAuditoria {
    @Id
    public UUID id;

    public UUID eventId;
    public UUID aggregateId;
    public String eventType;
    public String correlationId;

    @Column(columnDefinition = "text")
    public String payload;

    public Instant recebidoEm;

    protected EventoAuditoria() {}

    public EventoAuditoria(UUID eventId, UUID aggregateId, String eventType, String correlationId, String payload) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.correlationId = correlationId;
        this.payload = payload;
        this.recebidoEm = Instant.now();
    }
}
