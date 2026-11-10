package com.nms.poller.emulator;

import java.time.Instant;

public record DoubleSample(String metric, Double value, Instant timestamp) implements MetricSample {
}
