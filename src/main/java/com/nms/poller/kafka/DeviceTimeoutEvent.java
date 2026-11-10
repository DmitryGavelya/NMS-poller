package com.nms.poller.kafka;

import java.time.Instant;

public record DeviceTimeoutEvent(String deviceId, Instant timestamp, PollStatus pollStatus) implements PollEvent {

    public static DeviceTimeoutEvent of(String deviceId, Instant timestamp) {
        return new DeviceTimeoutEvent(deviceId, timestamp, PollStatus.TIMEOUT);
    }
}
