package com.nms.poller.kafka;

import java.time.Instant;

public sealed interface PollEvent permits MetricEvent, DeviceTimeoutEvent {

    String deviceId();

    Instant timestamp();

    PollStatus pollStatus();
}
