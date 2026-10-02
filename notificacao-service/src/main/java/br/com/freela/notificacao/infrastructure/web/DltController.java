package br.com.freela.notificacao.infrastructure.web;

import br.com.freela.notificacao.infrastructure.persistence.FailedEvent;
import br.com.freela.notificacao.infrastructure.persistence.FailedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/dlt/falhas")
@RequiredArgsConstructor
public class DltController {
    private final FailedEventRepository repository;
    private final KafkaTemplate<Object, Object> template;

    @GetMapping
    public List<FailedEvent> listar() {
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<FailedEvent> buscar(@PathVariable("id") UUID id) {
        return repository.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/reprocessar")
    public ResponseEntity<FailedEvent> reprocessar(@PathVariable("id") UUID id) {
        return repository.findById(id).map(falha -> {
            log.info("notificacao.dlt.reprocessar.inicio id={} eventId={} topic={} startedAt={}",
                    falha.id, falha.eventId, falha.topic, System.currentTimeMillis());

            var record = new ProducerRecord<Object, Object>(falha.topic, falha.messageKey, falha.payload);
            addHeader(record, "eventId", falha.eventId);
            addHeader(record, "eventType", falha.eventType);
            addHeader(record, "correlationId", falha.correlationId);
            template.send(record);

            falha.reprocessed = true;
            var salvo = repository.save(falha);

            log.info("notificacao.dlt.reprocessar.sucesso id={} eventId={} topic={} endedAt={}",
                    falha.id, falha.eventId, falha.topic, System.currentTimeMillis());
            return ResponseEntity.ok(salvo);
        }).orElse(ResponseEntity.notFound().build());
    }

    private static void addHeader(ProducerRecord<Object, Object> record, String key, String value) {
        if (value != null) {
            record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
