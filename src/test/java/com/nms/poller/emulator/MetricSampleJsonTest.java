package com.nms.poller.emulator;

import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.exc.InvalidTypeIdException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricSampleJsonTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void picksSubtypeByTypeDiscriminator() {
        List<MetricSample> samples = mapper.readValue("""
                [{"type":"DOUBLE","metric":"cpu_load","value":42.5,"timestamp":"2026-10-20T12:00:00Z"},
                 {"type":"BOOLEAN","metric":"door_open","value":true,"timestamp":"2026-10-20T12:00:00Z"},
                 {"type":"STRING","metric":"firmware","value":"1.2.3"}]
                """, new TypeReference<>() {
        });

        Instant ts = Instant.parse("2026-10-20T12:00:00Z");
        assertThat(samples).containsExactly(
                new DoubleSample("cpu_load", 42.5, ts),
                new BooleanSample("door_open", true, ts),
                new StringSample("firmware", "1.2.3", null));
    }

    @Test
    void acceptsLowerCaseTypeAndIgnoresUnknownFields() {
        MetricSample sample = mapper.readValue(
                "{\"type\":\"double\",\"metric\":\"t\",\"value\":1,\"unit\":\"C\"}", MetricSample.class);

        assertThat(sample).isEqualTo(new DoubleSample("t", 1.0, null));
    }

    @Test
    void rejectsUnknownType() {
        assertThatThrownBy(() -> mapper.readValue("{\"type\":\"BLOB\",\"metric\":\"x\",\"value\":1}", MetricSample.class))
                .isInstanceOf(InvalidTypeIdException.class);
    }
}
