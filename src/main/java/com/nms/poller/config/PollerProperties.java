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
            @NotNull Duration metricTypesRefreshInterval,
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
            @NotNull Duration tick,
            @NotNull Duration defaultInterval,
            @Min(1) int concurrency
    ) {

        public Poll {
            requirePositive("tick", tick);
            requirePositive("default-interval", defaultInterval);
        }

        private static void requirePositive(String name, Duration value) {
            if (value != null && (value.isZero() || value.isNegative())) {
                throw new IllegalArgumentException("nms.poller.poll." + name + " must be positive");
            }
        }
    }

    public record Kafka(
            @Valid @NotNull Topics topics,
            @Min(1) int partitions
    ) {
    }

    public record Topics(
            @NotBlank String booleans,
            @NotBlank String numerics,
            @NotBlank String strings,
            @NotBlank String timeouts
    ) {
    }
}
