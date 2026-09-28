package br.com.freela.auditoria.application;

import java.util.UUID;

import br.com.freela.auditoria.infrastructure.persistence.EventoAuditoria;
import br.com.freela.auditoria.infrastructure.persistence.EventoAuditoriaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuditoriaService {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);
    private final EventoAuditoriaRepository repository;

    public AuditoriaService(EventoAuditoriaRepository repository) { this.repository = repository; }

    @Transactional
    public void registrar(UUID eventId, UUID aggregateId, String eventType, String correlationId, String payload) {
        if (repository.existsByEventId(eventId)) {
            log.info("auditoria.evento.duplicado.ignorado eventId={}", eventId);
            return;
        }
        log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={} startedAt={}",
                eventId, aggregateId, eventType, correlationId, System.currentTimeMillis());
        var evento = repository.save(new EventoAuditoria(eventId, aggregateId, eventType, correlationId, payload));

        log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={} endedAt={}",
                evento.id, eventId, aggregateId, eventType, System.currentTimeMillis());
    }
}
