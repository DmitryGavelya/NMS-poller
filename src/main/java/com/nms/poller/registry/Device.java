package com.nms.poller.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public record Device(Long id, String name, String deviceType, Boolean enabled) {

    public boolean isPollable() {
        return id != null && !Boolean.FALSE.equals(enabled);
    }
}
