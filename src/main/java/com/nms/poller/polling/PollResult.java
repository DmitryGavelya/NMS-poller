package com.nms.poller.polling;

import com.nms.poller.kafka.MetricEvent;
import com.nms.poller.kafka.PollEvent;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public record PollResult(PollRequest request, List<PollEvent> events, boolean success) {

    public Set<String> metricNames() {
        return events.stream()
                .filter(MetricEvent.class::isInstance)
                .map(e -> ((MetricEvent) e).metricTypeCode())
                .collect(Collectors.toSet());
    }
}
