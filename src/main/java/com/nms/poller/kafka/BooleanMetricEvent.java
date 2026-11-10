package com.nms.poller.kafka;

import java.time.Instant;

public record BooleanMetricEvent(
        long deviceId,
        String metricTypeCode,
        Boolean value,
        Instant timestamp,
        PollStatus pollStatus
) implements MetricEvent {

    public static BooleanMetricEvent success(long deviceId, String metricTypeCode, Boolean value, Instant timestamp) {
        return new BooleanMetricEvent(deviceId, metricTypeCode, value, timestamp, PollStatus.SUCCESS);
    }
}
