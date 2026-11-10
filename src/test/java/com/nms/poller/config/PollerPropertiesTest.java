package com.nms.poller.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollerPropertiesTest {

    @Test
    void rejectsNonPositiveIntervals() {
        assertThatThrownBy(() -> new PollerProperties.Poll(Duration.ofSeconds(1), Duration.ZERO, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("default-interval");
        assertThatThrownBy(() -> new PollerProperties.Poll(Duration.ofSeconds(-1), Duration.ofSeconds(30), 4))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
