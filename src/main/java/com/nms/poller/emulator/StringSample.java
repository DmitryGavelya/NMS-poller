package com.nms.poller.emulator;

import java.time.Instant;

public record StringSample(String metric, String value, Instant timestamp) implements MetricSample {
}
