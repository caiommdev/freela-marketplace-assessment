package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.StatusContrato;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ContratoResponse
        (
            UUID id,
            UUID clienteId,
            UUID freelancerId,
            String titulo,
            BigDecimal valor,
            StatusContrato status,
            Instant criadoEm
        ) {

    static ContratoResponse from(Contrato contrato) {
        return new ContratoResponse(
                contrato.id(),
                contrato.clienteId(),
                contrato.freelancerId(),
                contrato.titulo(),
                contrato.valor(),
                contrato.status(),
                contrato.criadoEm()
        );
    }
}
