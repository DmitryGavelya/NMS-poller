package com.nms.poller.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollerPropertiesTest {

    @Test
    void usesOverrideOrDefaultInterval() {
        var poll = new PollerProperties.Poll(Duration.ofSeconds(1), Duration.ofSeconds(30), 4,
                Map.of("uptime", Duration.ofMinutes(5)));

        assertThat(poll.intervalFor("uptime")).isEqualTo(Duration.ofMinutes(5));
        assertThat(poll.intervalFor("cpu_load")).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void nullOverridesMeanNoOverrides() {
        var poll = new PollerProperties.Poll(Duration.ofSeconds(1), Duration.ofSeconds(30), 4, null);

        assertThat(poll.metricIntervals()).isEmpty();
    }

    @Test
    void rejectsNonPositiveIntervals() {
        assertThatThrownBy(() -> new PollerProperties.Poll(Duration.ofSeconds(1), Duration.ofSeconds(30), 4,
                Map.of("uptime", Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uptime");
        assertThatThrownBy(() -> new PollerProperties.Poll(Duration.ofSeconds(-1), Duration.ofSeconds(30), 4, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
