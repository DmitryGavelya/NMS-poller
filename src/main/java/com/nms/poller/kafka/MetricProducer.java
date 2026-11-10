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

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PollerProperties.Topics topics;

    public MetricProducer(KafkaTemplate<String, Object> kafkaTemplate, PollerProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = props.kafka().topics();
    }

    public Mono<Boolean> send(PollEvent event) {
        String topic = topicFor(event);
        return Mono.fromFuture(() -> kafkaTemplate.send(topic, event.deviceId(), event))
                .map(result -> true)
                .onErrorResume(e -> {
                    log.error("Failed to publish event to {}: {} ({})", topic, event, e.toString());
                    return Mono.just(false);
                });
    }

    String topicFor(PollEvent event) {
        return switch (event) {
            case BooleanMetricEvent e -> topics.booleans();
            case DoubleMetricEvent e -> topics.doubles();
            case StringMetricEvent e -> topics.strings();
            case DeviceTimeoutEvent e -> topics.timeouts();
        };
    }
}
