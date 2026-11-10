package com.nms.poller.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public record Device(String id, String name, String deviceType, Boolean enabled) {

    public boolean isPollable() {
        return id != null && !id.isBlank() && !Boolean.FALSE.equals(enabled);
    }
}
