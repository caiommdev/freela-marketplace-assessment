package br.com.freela.contrato.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 2;

    @Bean
    NewTopic contratoCriadoTopic(
            @Value("${spring.kafka.topics.freela-marketplace.contrato.criado}") String name) {
        return TopicBuilder.name(name).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic entregaRegistradaTopic(
            @Value("${spring.kafka.topics.freela-marketplace.contrato.entrega-registrada}") String name) {
        return TopicBuilder.name(name).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic contratoConcluidoTopic(
            @Value("${spring.kafka.topics.freela-marketplace.contrato.concluido}") String name) {
        return TopicBuilder.name(name).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic contratoCanceladoTopic(
            @Value("${spring.kafka.topics.freela-marketplace.contrato.cancelado}") String name) {
        return TopicBuilder.name(name).partitions(PARTITIONS).replicas(REPLICAS).build();
    }
}
