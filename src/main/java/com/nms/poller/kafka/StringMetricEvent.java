package com.nms.poller.kafka;

import java.time.Instant;

public record StringMetricEvent(
        long deviceId,
        String metricTypeCode,
        String value,
        Instant timestamp,
        PollStatus pollStatus
) implements MetricEvent {

    public static StringMetricEvent success(long deviceId, String metricTypeCode, String value, Instant timestamp) {
        return new StringMetricEvent(deviceId, metricTypeCode, value, timestamp, PollStatus.SUCCESS);
    }
}
