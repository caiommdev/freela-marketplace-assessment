package br.com.freela.reputacao.infrastructure.persistence;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "reputacoes")
public class ReputacaoFreelancer {
    @Id
    public UUID freelancerId;

    public int contratosConcluidos;
    public BigDecimal valorTotal = BigDecimal.ZERO;

    protected ReputacaoFreelancer() {}

    public ReputacaoFreelancer(UUID freelancerId) { this.freelancerId = freelancerId; }

    public void registrarContrato(BigDecimal valor) {
        contratosConcluidos++;
        valorTotal = valorTotal.add(valor);
    }
}
