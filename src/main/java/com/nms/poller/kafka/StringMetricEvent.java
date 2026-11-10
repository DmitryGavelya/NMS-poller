package com.nms.poller.kafka;

import java.time.Instant;

public record StringMetricEvent(
        String deviceId,
        String metricType,
        String value,
        Instant timestamp,
        PollStatus pollStatus
) implements MetricEvent {

    public static StringMetricEvent success(String deviceId, String metricType, String value, Instant timestamp) {
        return new StringMetricEvent(deviceId, metricType, value, timestamp, PollStatus.SUCCESS);
    }
}
