package com.nms.poller.polling;

import com.nms.poller.TestProps;
import com.nms.poller.registry.Device;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PollQueueTest {

    private static final Instant T0 = Instant.parse("2026-10-27T10:00:00Z");

    private final Device router = new Device(1L, "Router", "ACCESS_ROUTER", true);
    private final Device lte = new Device(2L, "LTE", "LTE", true);
    private final PollQueue queue = new PollQueue(TestProps.props(Map.of(
            "door_open", Duration.ofSeconds(1),
            "uptime", Duration.ofMinutes(5))));

    @Test
    void newDeviceIsDiscoveredImmediatelyAndOnlyOnce() {
        queue.sync(List.of(router), T0);
        queue.sync(List.of(router), T0);

        assertThat(queue.pollDue(T0)).containsExactly(new PollRequest(router, Set.of()));
        assertThat(queue.pollDue(T0)).isEmpty();
    }

    @Test
    void failedDiscoveryIsRetriedAfterDefaultInterval() {
        queue.sync(List.of(router), T0);
        queue.pollDue(T0);

        assertThat(queue.pollDue(T0.plusSeconds(29))).isEmpty();
        assertThat(queue.pollDue(T0.plusSeconds(30))).extracting(PollRequest::isDiscovery).containsExactly(true);
    }

    @Test
    void pollsEachMetricWithItsOwnInterval() {
        queue.sync(List.of(router), T0);
        queue.pollDue(T0);
        queue.discovered(1L, List.of("door_open", "uptime", "cpu_load"), T0);

        assertThat(queue.pollDue(T0)).isEmpty();
        assertThat(queue.pollDue(T0.plusSeconds(1))).containsExactly(request(router, "door_open"));
        assertThat(queue.pollDue(T0.plusSeconds(2))).containsExactly(request(router, "door_open"));
        assertThat(queue.pollDue(T0.plusSeconds(30)))
                .containsExactly(request(router, "cpu_load", "door_open"));
        assertThat(queue.pollDue(T0.plusSeconds(31))).containsExactly(request(router, "door_open"));
        assertThat(queue.pollDue(T0.plusSeconds(300)))
                .containsExactly(request(router, "uptime", "cpu_load", "door_open"));
    }

    @Test
    void batchesMetricsOfOneDeviceButKeepsDevicesSeparate() {
        queue.sync(List.of(router, lte), T0);
        queue.pollDue(T0);
        queue.discovered(1L, List.of("cpu_load", "memory_load"), T0);
        queue.discovered(2L, List.of("cpu_load"), T0);

        assertThat(queue.pollDue(T0.plusSeconds(30))).containsExactlyInAnyOrder(
                request(router, "cpu_load", "memory_load"), request(lte, "cpu_load"));
    }

    @Test
    void keepsFixedGridWithoutDriftAndSkipsMissedSlots() {
        queue.sync(List.of(router), T0);
        queue.pollDue(T0);
        queue.discovered(1L, List.of("door_open"), T0);

        assertThat(queue.pollDue(T0.plusMillis(1400))).hasSize(1);
        assertThat(queue.pollDue(T0.plusMillis(1999))).isEmpty();
        assertThat(queue.pollDue(T0.plusSeconds(2))).hasSize(1);

        assertThat(queue.pollDue(T0.plusSeconds(12))).hasSize(1);
        assertThat(queue.pollDue(T0.plusSeconds(12))).isEmpty();
        assertThat(queue.pollDue(T0.plusSeconds(13))).hasSize(1);
    }

    @Test
    void removedDeviceLosesItsTasksAndReaddedDeviceIsDiscoveredAgain() {
        queue.sync(List.of(router, lte), T0);
        queue.pollDue(T0);
        queue.discovered(1L, List.of("door_open"), T0);
        queue.discovered(2L, List.of("door_open"), T0);
        assertThat(queue.size()).isEqualTo(2);

        queue.sync(List.of(lte), T0.plusSeconds(1));

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.pollDue(T0.plusSeconds(1))).containsExactly(request(lte, "door_open"));

        queue.sync(List.of(lte, router), T0.plusSeconds(2));
        assertThat(queue.pollDue(T0.plusSeconds(2)))
                .contains(new PollRequest(router, Set.of()));
    }

    @Test
    void discoveryOfUnknownDeviceOrWithoutMetricsIsIgnored() {
        queue.discovered(99L, List.of("cpu_load"), T0);
        queue.sync(List.of(router), T0);
        queue.discovered(1L, List.of(), T0);

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.pollDue(T0)).extracting(PollRequest::isDiscovery).containsExactly(true);
    }

    @Test
    void updatedDeviceDataIsUsedInRequests() {
        queue.sync(List.of(router), T0);
        queue.pollDue(T0);
        queue.discovered(1L, List.of("door_open"), T0);
        Device renamed = new Device(1L, "Renamed", "ACCESS_ROUTER", true);

        queue.sync(List.of(renamed), T0);

        assertThat(queue.pollDue(T0.plusSeconds(1))).containsExactly(request(renamed, "door_open"));
    }

    private static PollRequest request(Device device, String... metrics) {
        return new PollRequest(device, Set.of(metrics));
    }
}
