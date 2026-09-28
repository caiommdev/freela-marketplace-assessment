package br.com.freela.reputacao.infrastructure.web;

import br.com.freela.reputacao.infrastructure.persistence.ReputacaoFreelancer;
import br.com.freela.reputacao.infrastructure.persistence.ReputacaoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/reputacoes")
public class ReputacaoController {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoController.class);
    private final ReputacaoRepository repository;

    ReputacaoController(ReputacaoRepository repository) { this.repository = repository; }

    public record ReputacaoRequest(int contratosConcluidos, BigDecimal valorTotal) {}

    @GetMapping
    public List<ReputacaoFreelancer> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.reputacao.listar correlationId={}", correlationId);
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<ReputacaoFreelancer> buscar(@PathVariable("id") UUID id) {
        return repository.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<ReputacaoFreelancer> atualizar(@PathVariable("id") UUID id, @RequestBody ReputacaoRequest req) {
        var r = repository.findById(id).orElseGet(() -> new ReputacaoFreelancer(id));
        r.contratosConcluidos = req.contratosConcluidos();
        r.valorTotal = req.valorTotal();
        return ResponseEntity.ok(repository.save(r));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> remover(@PathVariable("id") UUID id) {
        if (!repository.existsById(id)) return ResponseEntity.notFound().build();
        repository.deleteById(id);
        return ResponseEntity.noContent().build();
    }
}
