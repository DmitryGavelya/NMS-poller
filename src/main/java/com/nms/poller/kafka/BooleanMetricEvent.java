package com.nms.poller.kafka;

import java.time.Instant;

public record BooleanMetricEvent(
        String deviceId,
        String metricType,
        Boolean value,
        Instant timestamp,
        PollStatus pollStatus
) implements MetricEvent {

    public static BooleanMetricEvent success(String deviceId, String metricType, Boolean value, Instant timestamp) {
        return new BooleanMetricEvent(deviceId, metricType, value, timestamp, PollStatus.SUCCESS);
    }
}
