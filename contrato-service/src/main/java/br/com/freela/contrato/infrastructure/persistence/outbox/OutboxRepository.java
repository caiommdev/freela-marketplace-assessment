package br.com.freela.contrato.infrastructure.persistence.outbox;

import br.com.freela.contrato.infrastructure.event.outbox.OutboxEvent;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
    List<OutboxEvent> findBySentAtIsNullOrderBySeqAsc(Limit limit);
}