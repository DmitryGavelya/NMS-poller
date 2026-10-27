package com.nms.poller.kafka;

import java.time.Instant;

public record MetricEvent(
        String deviceId,
        String metricType,
        Double value,
        Instant timestamp,
        PollStatus pollStatus
) {

    public static MetricEvent success(String deviceId, String metricType, Double value, Instant timestamp) {
        return new MetricEvent(deviceId, metricType, value, timestamp, PollStatus.SUCCESS);
    }

    public static MetricEvent timeout(String deviceId, Instant timestamp) {
        return new MetricEvent(deviceId, null, null, timestamp, PollStatus.TIMEOUT);
    }
}
