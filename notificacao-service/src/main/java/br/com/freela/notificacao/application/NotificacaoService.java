package br.com.freela.notificacao.application;

import br.com.freela.notificacao.infrastructure.persistence.Notificacao;
import br.com.freela.notificacao.infrastructure.persistence.NotificacaoRepository;
import br.com.freela.notificacao.infrastructure.persistence.ProcessedEvent;
import br.com.freela.notificacao.infrastructure.persistence.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class NotificacaoService {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoService.class);
    private final NotificacaoRepository repository;
    private final ProcessedEventRepository processedRepository;

    public NotificacaoService(NotificacaoRepository repository, ProcessedEventRepository processedRepository) {
        this.repository = repository;
        this.processedRepository = processedRepository;
    }

    @Transactional
    public void registrar(UUID eventId, UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {
        if (processedRepository.existsById(eventId)) {
            log.info("notificacao.evento.duplicado.ignorado eventId={} contratoId={}", eventId, contratoId);
            return;
        }

        log.info("notificacao.registro.inicio eventId={} contratoId={} destinatarioId={} tipo={} startedAt={}",
                eventId, contratoId, destinatarioId, tipo, System.currentTimeMillis());

        var notificacao = repository.save(new Notificacao(contratoId, destinatarioId, tipo, mensagem));
        processedRepository.save(new ProcessedEvent(eventId));

        log.info("\n========================= NOTIFICACAO =========================\n"
                + " tipo         : {}\n"
                + " contratoId   : {}\n"
                + " destinatario : {}\n"
                + " mensagem     : {}\n"
                + "===============================================================",
                tipo, contratoId, destinatarioId, mensagem);

        log.info("notificacao.registro.sucesso notificacaoId={} eventId={} contratoId={} destinatarioId={} endedAt={}",
                notificacao.id, eventId, contratoId, destinatarioId, System.currentTimeMillis());
    }
}
