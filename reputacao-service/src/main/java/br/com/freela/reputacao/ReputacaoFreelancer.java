package br.com.freela.reputacao;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name="reputacoes")
class ReputacaoFreelancer {
    @Id
    UUID freelancerId;

    int contratosConcluidos;
    BigDecimal valorTotal = BigDecimal.ZERO;

    protected ReputacaoFreelancer() {}

    ReputacaoFreelancer(UUID freelancerId) { this.freelancerId = freelancerId; }

    void registrarContrato(BigDecimal valor) {
        contratosConcluidos++;
        valorTotal = valorTotal.add(valor);
    }
}
