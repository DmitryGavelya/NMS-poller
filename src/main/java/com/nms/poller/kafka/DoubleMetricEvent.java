package com.nms.poller.kafka;

import java.time.Instant;

public record DoubleMetricEvent(
        String deviceId,
        String metricType,
        Double value,
        Instant timestamp,
        PollStatus pollStatus
) implements MetricEvent {

    public static DoubleMetricEvent success(String deviceId, String metricType, Double value, Instant timestamp) {
        return new DoubleMetricEvent(deviceId, metricType, value, timestamp, PollStatus.SUCCESS);
    }
}
