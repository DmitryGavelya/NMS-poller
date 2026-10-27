package com.nms.poller.kafka;

import com.nms.poller.config.PollerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class MetricProducer {

    private static final Logger log = LoggerFactory.getLogger(MetricProducer.class);

    private final KafkaTemplate<String, MetricEvent> kafkaTemplate;
    private final String topic;

    public MetricProducer(KafkaTemplate<String, MetricEvent> kafkaTemplate, PollerProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = props.kafka().topic();
    }

    public Mono<Boolean> send(MetricEvent event) {
        return Mono.fromFuture(() -> kafkaTemplate.send(topic, event.deviceId(), event))
                .map(result -> true)
                .onErrorResume(e -> {
                    log.error("Failed to publish metric event {}: {}", event, e.toString());
                    return Mono.just(false);
                });
    }
}
