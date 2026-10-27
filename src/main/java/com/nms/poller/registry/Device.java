package com.nms.poller.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Устройство из реестра NMS Backend ({@code GET /api/devices}).
 * Poller'у нужен только идентификатор; остальные поля — для логов.
 * Неизвестные поля игнорируются, чтобы не ломаться при развитии контракта backend.
 *
 * @param enabled {@code null} трактуется как «включено»
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Device(String id, String name, String deviceType, Boolean enabled) {

    public boolean isPollable() {
        return id != null && !id.isBlank() && !Boolean.FALSE.equals(enabled);
    }
}
