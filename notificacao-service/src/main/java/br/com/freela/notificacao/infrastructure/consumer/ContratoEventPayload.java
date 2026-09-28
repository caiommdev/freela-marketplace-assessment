package br.com.freela.notificacao.infrastructure.consumer;

import java.util.UUID;

public record ContratoEventPayload(
        String eventId,
        String occurredAt,
        UUID contratoId,
        String clienteId,
        String freelancerId,
        String titulo,
        String valor
) {}
