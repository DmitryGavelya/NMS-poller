package com.nms.poller.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin.NewTopics metricTopics(PollerProperties props) {
        PollerProperties.Topics topics = props.kafka().topics();
        int partitions = props.kafka().partitions();
        return new KafkaAdmin.NewTopics(
                topic(topics.booleans(), partitions),
                topic(topics.doubles(), partitions),
                topic(topics.strings(), partitions),
                topic(topics.timeouts(), partitions));
    }

    private static NewTopic topic(String name, int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }
}
