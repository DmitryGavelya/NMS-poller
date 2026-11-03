package com.nms.poller.kafka;

public sealed interface MetricEvent extends PollEvent permits BooleanMetricEvent, DoubleMetricEvent, StringMetricEvent {

    String metricType();

    Object value();
}
