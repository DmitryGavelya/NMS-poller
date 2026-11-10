package com.nms.poller.emulator;

import java.time.Instant;

public record BooleanSample(String metric, Boolean value, Instant timestamp) implements MetricSample {
}
