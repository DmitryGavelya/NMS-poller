package com.nms.poller.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricType(String code, Integer pollIntervalSec) {

    public boolean hasValidInterval() {
        return code != null && !code.isBlank() && pollIntervalSec != null && pollIntervalSec > 0;
    }
}
