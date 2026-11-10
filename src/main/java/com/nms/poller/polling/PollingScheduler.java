package com.nms.poller.polling;

import com.nms.poller.config.PollerProperties;
import com.nms.poller.kafka.MetricProducer;
import com.nms.poller.registry.DeviceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

@Component
public class PollingScheduler {

    private static final Logger log = LoggerFactory.getLogger(PollingScheduler.class);

    private final DeviceRegistry registry;
    private final PollQueue queue;
    private final DevicePoller poller;
    private final MetricProducer producer;
    private final PollerProperties props;
    private final Clock clock;

    public PollingScheduler(
            DeviceRegistry registry,
            PollQueue queue,
            DevicePoller poller,
            MetricProducer producer,
            PollerProperties props,
            Clock clock
    ) {
        this.registry = registry;
        this.queue = queue;
        this.poller = poller;
        this.producer = producer;
        this.props = props;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${nms.poller.poll.tick}")
    public void tick() {
        Instant now = clock.instant();
        queue.sync(registry.devices(), now);
        List<PollRequest> due = queue.pollDue(now);
        if (due.isEmpty()) {
            log.trace("Nothing due");
            return;
        }
        PollCycleStats stats = runCycle(due);
        log.info("Poll cycle finished: devices={}, published={}, failedToPublish={}",
                due.size(), stats.published(), stats.failed());
    }

    PollCycleStats runCycle(List<PollRequest> requests) {
        List<Boolean> results = Flux.fromIterable(requests)
                .flatMap(poller::poll, props.poll().concurrency())
                .doOnNext(this::registerDiscovered)
                .flatMapIterable(PollResult::events)
                .flatMap(producer::send)
                .collectList()
                .block();
        if (results == null) {
            return new PollCycleStats(0, 0);
        }
        long published = results.stream().filter(Boolean::booleanValue).count();
        return new PollCycleStats(published, results.size() - published);
    }

    private void registerDiscovered(PollResult result) {
        if (result.success() && result.request().isDiscovery()) {
            queue.discovered(result.request().device().id(), result.metricNames(), clock.instant());
        }
    }

    record PollCycleStats(long published, long failed) {
    }
}
