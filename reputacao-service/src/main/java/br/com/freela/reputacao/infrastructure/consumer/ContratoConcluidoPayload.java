package br.com.freela.reputacao.infrastructure.consumer;

import java.util.UUID;

public record ContratoConcluidoPayload(
        String eventId,
        String occurredAt,
        UUID contratoId,
        String clienteId,
        String freelancerId,
        String valor
) {}
