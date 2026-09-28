package br.com.freela.reputacao.infrastructure.consumer;

import br.com.freela.reputacao.application.ReputacaoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class ReputacaoListener {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoListener.class);
    private final ReputacaoService service;
    private final ObjectMapper objectMapper;

    ReputacaoListener(ReputacaoService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "freela-marketplace.contrato.concluido")
    public void onConcluido(
            @Payload String payload,
            @Header("eventId") String eventId,
            @Header("eventType") String eventType,
            @Header("correlationId") String correlationId,
            @Header(KafkaHeaders.RECEIVED_KEY) String contratoId) {

        MDC.put("correlationId", correlationId);
        try {
            log.info("reputacao.evento.recebido eventId={} eventType={} contratoId={} startedAt={}",
                    eventId, eventType, contratoId, System.currentTimeMillis());

            var dados = objectMapper.readValue(payload, ContratoConcluidoPayload.class);
            service.registrarContratoConcluido(
                    UUID.fromString(eventId),
                    dados.contratoId(),
                    UUID.fromString(dados.freelancerId()),
                    new BigDecimal(dados.valor()));
        } finally {
            MDC.clear();
        }
    }
}
