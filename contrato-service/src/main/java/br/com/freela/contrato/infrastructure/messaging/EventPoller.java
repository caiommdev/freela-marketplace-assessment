package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.infrastructure.event.outbox.OutboxEvent;
import br.com.freela.contrato.infrastructure.persistence.outbox.OutboxRepository;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@AllArgsConstructor
public class EventPoller {
    private static final Logger log = LoggerFactory.getLogger(EventPoller.class);

    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPending() {
        var pendentes = outboxRepository.findBySentAtIsNullOrderBySeqAsc(Limit.of(100));
        if (pendentes.isEmpty()) return;

        for (OutboxEvent e : pendentes) {
            try {
                log.info("outbox.evento.publicado eventId={} eventType={} contratoId={} topic={}",
                        e.getId(), e.getEventType(), e.getAggregateId(), e.getTopic());
                eventPublisher.publishOutbox(e, e.getTopic(), e.getCorrelationId());
            } catch (Exception ex) {
                log.error("outbox.publicacao.falhou eventId={} — será retentado", e.getId(), ex);
            }
        }
    }
}