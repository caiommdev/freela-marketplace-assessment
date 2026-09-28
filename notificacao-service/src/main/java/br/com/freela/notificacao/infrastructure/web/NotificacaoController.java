package br.com.freela.notificacao.infrastructure.web;

import br.com.freela.notificacao.infrastructure.persistence.Notificacao;
import br.com.freela.notificacao.infrastructure.persistence.NotificacaoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/notificacoes")
public class NotificacaoController {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoController.class);
    private final NotificacaoRepository repository;

    NotificacaoController(NotificacaoRepository repository) { this.repository = repository; }

    public record NotificacaoRequest(UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {}

    @GetMapping
    public List<Notificacao> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.notificacao.listar correlationId={}", correlationId);
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Notificacao> buscar(@PathVariable("id") UUID id) {
        return repository.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<Notificacao> criar(@RequestBody NotificacaoRequest req) {
        var n = repository.save(new Notificacao(req.contratoId(), req.destinatarioId(), req.tipo(), req.mensagem()));
        return ResponseEntity.status(201).body(n);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Notificacao> atualizar(@PathVariable("id") UUID id, @RequestBody NotificacaoRequest req) {
        return repository.findById(id).map(n -> {
            n.contratoId = req.contratoId();
            n.destinatarioId = req.destinatarioId();
            n.tipo = req.tipo();
            n.mensagem = req.mensagem();
            return ResponseEntity.ok(repository.save(n));
        }).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> remover(@PathVariable("id") UUID id) {
        if (!repository.existsById(id)) return ResponseEntity.notFound().build();
        repository.deleteById(id);
        return ResponseEntity.noContent().build();
    }
}
