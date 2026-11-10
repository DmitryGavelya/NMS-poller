package com.nms.poller.registry;

import com.nms.poller.config.PollerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class MetricTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(MetricTypeRegistry.class);

    private final MetricTypeRegistryClient client;
    private final PollerProperties props;
    private final AtomicReference<Map<String, Duration>> intervals = new AtomicReference<>(Map.of());

    public MetricTypeRegistry(MetricTypeRegistryClient client, PollerProperties props) {
        this.client = client;
        this.props = props;
    }

    public Duration intervalFor(String metricTypeCode) {
        return intervals.get().getOrDefault(metricTypeCode, props.poll().defaultInterval());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initialLoad() {
        refresh();
    }

    @Scheduled(fixedDelayString = "${nms.poller.backend.metric-types-refresh-interval}",
            initialDelayString = "${nms.poller.backend.metric-types-refresh-interval}")
    public void refresh() {
        try {
            List<MetricType> fetched = client.fetchMetricTypes()
                    .block(props.backend().timeout().multipliedBy(2));
            Map<String, Duration> loaded = new HashMap<>();
            if (fetched != null) {
                for (MetricType type : fetched) {
                    if (type.hasValidInterval()) {
                        loaded.put(type.code(), Duration.ofSeconds(type.pollIntervalSec()));
                    } else {
                        log.warn("Ignoring metric type with invalid poll interval: {}", type);
                    }
                }
            }
            Map<String, Duration> previous = intervals.getAndSet(Map.copyOf(loaded));
            if (!previous.equals(loaded)) {
                log.info("Metric type intervals updated: {} metric types", loaded.size());
            }
        } catch (Exception e) {
            log.warn("Failed to refresh metric types, keeping {} cached intervals: {}",
                    intervals.get().size(), e.toString());
        }
    }
}
