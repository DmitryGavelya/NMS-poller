package com.nms.poller.emulator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricSample(String metric, Double value, Instant timestamp) {

    @JsonCreator
    public static MetricSample of(@JsonProperty("metric") String metric,
                                  @JsonProperty("value") JsonNode value,
                                  @JsonProperty("timestamp") Instant timestamp) {
        return new MetricSample(metric, toDouble(value), timestamp);
    }

    private static Double toDouble(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isBoolean()) {
            return node.booleanValue() ? 1.0 : 0.0;
        }
        if (node.isNumber()) {
            return node.doubleValue();
        }
        if (node.isTextual()) {
            try {
                    return Double.valueOf(node.textValue());
                } catch (NumberFormatException e) {
                    return null;
                }
        }
        return null;
    }
}
