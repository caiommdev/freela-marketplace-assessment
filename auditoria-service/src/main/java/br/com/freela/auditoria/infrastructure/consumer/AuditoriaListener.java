package br.com.freela.auditoria.infrastructure.consumer;

import br.com.freela.auditoria.application.AuditoriaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class AuditoriaListener {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaListener.class);
    private final AuditoriaService service;

    AuditoriaListener(AuditoriaService service) { this.service = service; }

    @KafkaListener(topics = {
            "freela-marketplace.contrato.criado",
            "freela-marketplace.contrato.entrega-registrada",
            "freela-marketplace.contrato.concluido",
            "freela-marketplace.contrato.cancelado"
    })
    public void onEvento(
            @Payload String payload,
            @Header("eventId") String eventId,
            @Header("eventType") String eventType,
            @Header("correlationId") String correlationId,
            @Header(KafkaHeaders.RECEIVED_KEY) String contratoId,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        MDC.put("correlationId", correlationId);
        try {
            log.info("auditoria.evento.recebido eventId={} eventType={} contratoId={} topic={} startedAt={}",
                    eventId, eventType, contratoId, topic, System.currentTimeMillis());
            service.registrar(UUID.fromString(eventId), UUID.fromString(contratoId),
                    eventType, correlationId, payload);
        } finally {
            MDC.clear();
        }
    }
}
