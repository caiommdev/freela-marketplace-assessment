package br.com.freela.auditoria.infrastructure.web;

import br.com.freela.auditoria.infrastructure.persistence.EventoAuditoria;
import br.com.freela.auditoria.infrastructure.persistence.EventoAuditoriaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaController.class);
    private final EventoAuditoriaRepository repository;

    AuditoriaController(EventoAuditoriaRepository repository) { this.repository = repository; }

    @GetMapping
    public List<EventoAuditoria> listar(@RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        log.info("http.auditoria.listar correlationId={}", correlationId);
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<EventoAuditoria> buscar(@PathVariable("id") UUID id) {
        return repository.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> remover(@PathVariable("id") UUID id) {
        if (!repository.existsById(id)) return ResponseEntity.notFound().build();
        repository.deleteById(id);
        return ResponseEntity.noContent().build();
    }
}
