package com.nms.poller.polling;

import com.nms.poller.config.PollerProperties;
import com.nms.poller.kafka.MetricProducer;
import com.nms.poller.registry.Device;
import com.nms.poller.registry.DeviceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;

@Component
public class PollingScheduler {

    private static final Logger log = LoggerFactory.getLogger(PollingScheduler.class);

    private final DeviceRegistry registry;
    private final DevicePoller poller;
    private final MetricProducer producer;
    private final PollerProperties props;

    public PollingScheduler(
        DeviceRegistry registry,
        DevicePoller poller,
        MetricProducer producer,
        PollerProperties props
    ) {
        this.registry = registry;
        this.poller = poller;
        this.producer = producer;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${nms.poller.poll.interval}")
    public void pollAll() {
        List<Device> devices = registry.devices();
        if (devices.isEmpty()) {
            log.debug("No devices to poll");
            return;
        }
        PollCycleStats stats = runCycle(devices);
        log.info("Poll cycle finished: devices={}, published={}, failedToPublish={}",
                devices.size(), stats.published(), stats.failed());
    }

    PollCycleStats runCycle(List<Device> devices) {
        List<Boolean> results = Flux.fromIterable(devices)
                .flatMap(poller::poll, props.poll().concurrency())
                .flatMapIterable(events -> events)
                .flatMap(producer::send)
                .collectList()
                .block();
        if (results == null) {
            return new PollCycleStats(0, 0);
        }
        long published = results.stream().filter(Boolean::booleanValue).count();
        return new PollCycleStats(published, results.size() - published);
    }

    record PollCycleStats(long published, long failed) {
    }
}
