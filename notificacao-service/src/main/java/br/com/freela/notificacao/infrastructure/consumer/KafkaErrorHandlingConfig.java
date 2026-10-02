package br.com.freela.notificacao.infrastructure.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;

@Slf4j
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(FailedEventRecoverer recoverer) {
        var backOff = new ExponentialBackOff();
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000L);
        backOff.setMaxAttempts(3);

        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);

        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("notificacao.evento.retry tentativa={} topic={} partition={} offset={} erro={} mensagem={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(),
                        ex.getClass().getSimpleName(), ex.getMessage()));

        return handler;
    }
}
