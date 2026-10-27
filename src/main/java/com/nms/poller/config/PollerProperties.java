package com.nms.poller.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Настройки poller'а (префикс {@code nms.poller}).
 */
@Validated
@ConfigurationProperties(prefix = "nms.poller")
public record PollerProperties(
        @Valid @NotNull Backend backend,
        @Valid @NotNull Emulator emulator,
        @Valid @NotNull Poll poll,
        @Valid @NotNull Kafka kafka
) {

    /** Реестр устройств в NMS Backend: GET {baseUrl}/api/devices. */
    public record Backend(
            @NotBlank String baseUrl,
            @NotNull Duration registryRefreshInterval,
            @NotNull Duration timeout
    ) {
    }

    /** Эмулятор устройств: GET {baseUrl}/devices/{id}/metrics. */
    public record Emulator(
            @NotBlank String baseUrl,
            @NotNull Duration connectTimeout,
            /* таймаут одной попытки опроса */
            @NotNull Duration timeout,
            @Min(0) int retries,
            @NotNull Duration retryBackoff
    ) {
    }

    /** Цикл опроса. */
    public record Poll(
            @NotNull Duration interval,
            /* сколько устройств опрашиваем параллельно */
            @Min(1) int concurrency
    ) {
    }

    public record Kafka(
            @NotBlank String topic,
            @Min(1) int partitions
    ) {
    }
}
