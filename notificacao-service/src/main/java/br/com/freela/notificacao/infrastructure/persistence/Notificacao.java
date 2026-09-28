package br.com.freela.notificacao.infrastructure.persistence;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notificacoes")
public class Notificacao {
    @Id
    public UUID id;

    public UUID contratoId;
    public UUID destinatarioId;
    public String tipo;

    @Column(length = 500)
    public String mensagem;

    public Instant criadaEm;

    protected Notificacao() {}

    public Notificacao(UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {
        this.id = UUID.randomUUID();
        this.contratoId = contratoId;
        this.destinatarioId = destinatarioId;
        this.tipo = tipo;
        this.mensagem = mensagem;
        this.criadaEm = Instant.now();
    }
}
