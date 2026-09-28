package br.com.freela.notificacao.infrastructure.consumer;

import br.com.freela.notificacao.application.NotificacaoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Component
public class NotificacaoListener {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoListener.class);
    private final NotificacaoService service;
    private final ObjectMapper objectMapper;

    NotificacaoListener(NotificacaoService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

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
            @Header(KafkaHeaders.RECEIVED_KEY) String contratoId) {

        MDC.put("correlationId", correlationId);
        try {
            log.info("notificacao.evento.recebido eventId={} eventType={} contratoId={} startedAt={}",
                    eventId, eventType, contratoId, System.currentTimeMillis());

            var dados = objectMapper.readValue(payload, ContratoEventPayload.class);
            UUID id = UUID.fromString(eventId);

            switch (eventType) {
                case "ContratoCriado" -> service.registrar(id, dados.contratoId(),
                        UUID.fromString(dados.freelancerId()), eventType,
                        "Voce recebeu um novo contrato: " + dados.titulo());
                case "EntregaRegistrada" -> service.registrar(id, dados.contratoId(),
                        UUID.fromString(dados.clienteId()), eventType,
                        "Uma entrega foi registrada no contrato " + dados.contratoId());
                case "ContratoConcluido" -> service.registrar(id, dados.contratoId(),
                        UUID.fromString(dados.freelancerId()), eventType,
                        "Contrato concluido. Valor: " + dados.valor());
                case "ContratoCancelado" -> service.registrar(id, dados.contratoId(),
                        UUID.fromString(dados.freelancerId()), eventType,
                        "O contrato " + dados.contratoId() + " foi cancelado");
                default -> log.warn("notificacao.evento.nao.tratado eventType={} eventId={}", eventType, eventId);
            }
        } finally {
            MDC.clear();
        }
    }
}
