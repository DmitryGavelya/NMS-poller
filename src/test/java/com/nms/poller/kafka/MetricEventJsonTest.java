package com.nms.poller.kafka;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MetricEventJsonTest {

    private static final Instant TS = Instant.parse("2026-10-27T10:00:00Z");

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void numericEventMatchesBackendContract() {
        JsonNode json = mapper.valueToTree(DoubleMetricEvent.success(42L, "CPU_LOAD", 55.5, TS));

        assertThat(json.get("deviceId").isIntegralNumber()).isTrue();
        assertThat(json.get("deviceId").asLong()).isEqualTo(42L);
        assertThat(json.get("metricTypeCode").asString()).isEqualTo("CPU_LOAD");
        assertThat(json.get("value").asDouble()).isEqualTo(55.5);
        assertThat(json.get("pollStatus").asString()).isEqualTo("SUCCESS");
        assertThat(json.has("metricType")).isFalse();
    }

    @Test
    void booleanAndStringEventsKeepPlainValueField() {
        JsonNode bool = mapper.valueToTree(BooleanMetricEvent.success(1L, "DOOR_OPEN", true, TS));
        JsonNode str = mapper.valueToTree(StringMetricEvent.success(1L, "FIRMWARE", "1.2.3", TS));

        assertThat(bool.get("value").isBoolean()).isTrue();
        assertThat(str.get("value").asString()).isEqualTo("1.2.3");
    }
}
