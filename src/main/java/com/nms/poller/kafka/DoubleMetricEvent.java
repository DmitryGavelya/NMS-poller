package com.nms.poller.kafka;

import java.time.Instant;

public record DoubleMetricEvent(
        long deviceId,
        String metricTypeCode,
        Double value,
        Instant timestamp,
        PollStatus pollStatus
) implements MetricEvent {

    public static DoubleMetricEvent success(long deviceId, String metricTypeCode, Double value, Instant timestamp) {
        return new DoubleMetricEvent(deviceId, metricTypeCode, value, timestamp, PollStatus.SUCCESS);
    }
}
