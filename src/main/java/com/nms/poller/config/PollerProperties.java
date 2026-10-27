package com.nms.poller.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "nms.poller")
public record PollerProperties(
        @Valid @NotNull Backend backend,
        @Valid @NotNull Emulator emulator,
        @Valid @NotNull Poll poll,
        @Valid @NotNull Kafka kafka
) {

    public record Backend(
            @NotBlank String baseUrl,
            @NotNull Duration registryRefreshInterval,
            @NotNull Duration timeout
    ) {
    }

    public record Emulator(
            @NotBlank String baseUrl,
            @NotNull Duration connectTimeout,
            @NotNull Duration timeout,
            @Min(0) int retries,
            @NotNull Duration retryBackoff
    ) {
    }

    public record Poll(
            @NotNull Duration interval,
            @Min(1) int concurrency
    ) {
    }

    public record Kafka(
            @NotBlank String topic,
            @Min(1) int partitions
    ) {
    }
}
