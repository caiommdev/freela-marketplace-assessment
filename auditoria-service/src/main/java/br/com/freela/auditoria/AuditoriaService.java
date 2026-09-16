package br.com.freela.auditoria;

import java.util.UUID;

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
        log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}",
                eventId, aggregateId, eventType, correlationId);
        var evento = repository.save(new EventoAuditoria(eventId, aggregateId, eventType, correlationId, payload));

        log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={}",
                evento.id, eventId, aggregateId, eventType);
    }
}
