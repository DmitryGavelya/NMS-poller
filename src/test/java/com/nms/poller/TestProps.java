package com.nms.poller;

import com.nms.poller.config.PollerProperties;

import java.time.Duration;
import java.util.Map;

public final class TestProps {

    private TestProps() {
    }

    public static PollerProperties.Topics topics() {
        return new PollerProperties.Topics("t.bool", "t.numeric", "t.string", "t.timeout");
    }

    public static PollerProperties props(Map<String, Duration> metricIntervals) {
        return props("http://127.0.0.1", 0, metricIntervals);
    }

    public static PollerProperties props(String emulatorUrl, int retries, Map<String, Duration> metricIntervals) {
        return new PollerProperties(
                new PollerProperties.Backend("http://backend", Duration.ofMinutes(1), Duration.ofSeconds(1)),
                new PollerProperties.Emulator(emulatorUrl, Duration.ofMillis(500), Duration.ofMillis(200), retries,
                        Duration.ofMillis(10)),
                new PollerProperties.Poll(Duration.ofSeconds(1), Duration.ofSeconds(30), 4, metricIntervals),
                new PollerProperties.Kafka(topics(), 1));
    }
}
