package com.nms.poller.registry;

import com.nms.poller.TestProps;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MetricTypeRegistryTest {

    private final MetricTypeRegistryClient client = mock(MetricTypeRegistryClient.class);
    private final MetricTypeRegistry registry = new MetricTypeRegistry(client, TestProps.props());

    @Test
    void usesBackendIntervalOrDefaultForUnknownMetric() {
        when(client.fetchMetricTypes()).thenReturn(Mono.just(List.of(new MetricType("CPU_LOAD", 5))));

        registry.refresh();

        assertThat(registry.intervalFor("CPU_LOAD")).isEqualTo(Duration.ofSeconds(5));
        assertThat(registry.intervalFor("UPTIME")).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void usesDefaultBeforeFirstLoad() {
        assertThat(registry.intervalFor("CPU_LOAD")).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void ignoresInvalidIntervals() {
        when(client.fetchMetricTypes()).thenReturn(Mono.just(List.of(
                new MetricType("ZERO", 0),
                new MetricType("NEGATIVE", -5),
                new MetricType("MISSING", null),
                new MetricType(" ", 10),
                new MetricType(null, 10),
                new MetricType("CPU_LOAD", 7))));

        registry.refresh();

        assertThat(registry.intervalFor("ZERO")).isEqualTo(Duration.ofSeconds(30));
        assertThat(registry.intervalFor("NEGATIVE")).isEqualTo(Duration.ofSeconds(30));
        assertThat(registry.intervalFor("MISSING")).isEqualTo(Duration.ofSeconds(30));
        assertThat(registry.intervalFor("CPU_LOAD")).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    void keepsCachedIntervalsWhenRefreshFails() {
        when(client.fetchMetricTypes()).thenReturn(Mono.just(List.of(new MetricType("CPU_LOAD", 5))));
        registry.refresh();

        when(client.fetchMetricTypes()).thenReturn(Mono.error(new IllegalStateException("backend down")));
        registry.refresh();

        assertThat(registry.intervalFor("CPU_LOAD")).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void picksUpChangedIntervals() {
        when(client.fetchMetricTypes()).thenReturn(Mono.just(List.of(new MetricType("CPU_LOAD", 5))));
        registry.refresh();

        when(client.fetchMetricTypes()).thenReturn(Mono.just(List.of(new MetricType("CPU_LOAD", 15))));
        registry.refresh();

        assertThat(registry.intervalFor("CPU_LOAD")).isEqualTo(Duration.ofSeconds(15));
    }
}
