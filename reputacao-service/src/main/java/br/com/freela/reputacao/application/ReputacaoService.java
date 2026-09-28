package br.com.freela.reputacao.application;

import br.com.freela.reputacao.infrastructure.persistence.ProcessedEvent;
import br.com.freela.reputacao.infrastructure.persistence.ProcessedEventRepository;
import br.com.freela.reputacao.infrastructure.persistence.ReputacaoFreelancer;
import br.com.freela.reputacao.infrastructure.persistence.ReputacaoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;

@Service
public class ReputacaoService {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoService.class);
    private final ReputacaoRepository repository;
    private final ProcessedEventRepository processedRepository;

    public ReputacaoService(ReputacaoRepository repository, ProcessedEventRepository processedRepository) {
        this.repository = repository;
        this.processedRepository = processedRepository;
    }

    @Transactional
    public void registrarContratoConcluido(UUID eventId, UUID contratoId, UUID freelancerId, BigDecimal valor) {
        if (processedRepository.existsById(eventId)) {
            log.info("reputacao.evento.duplicado.ignorado eventId={} contratoId={}", eventId, contratoId);
            return;
        }
        log.info("reputacao.atualizacao.inicio eventId={} contratoId={} freelancerId={} valor={}",
                eventId, contratoId, freelancerId, valor);

        var reputacao = repository.findById(freelancerId).orElseGet(() -> new ReputacaoFreelancer(freelancerId));
        reputacao.registrarContrato(valor);
        repository.save(reputacao);
        processedRepository.save(new ProcessedEvent(eventId));

        log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}",
                eventId, contratoId, freelancerId, reputacao.contratosConcluidos, reputacao.valorTotal);
    }
}
