package com.nms.poller.polling;

import com.nms.poller.registry.Device;

import java.util.Set;

public record PollRequest(Device device, Set<String> metrics) {

    public boolean isDiscovery() {
        return metrics.isEmpty();
    }
}
