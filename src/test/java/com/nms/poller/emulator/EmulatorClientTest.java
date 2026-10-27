package com.nms.poller.emulator;

import com.nms.poller.config.PollerProperties;
import com.nms.poller.config.WebClientConfig;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class EmulatorClientTest {

    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void parsesNumericAndBooleanSamples() {
        respond("/devices/router-1/metrics", 200, """
                [{"metric":"cpu_load","value":42.5,"timestamp":"2026-10-20T12:00:00Z"},
                 {"metric":"door_open","value":true,"timestamp":"2026-10-20T12:00:00Z"}]
                """);

        StepVerifier.create(client(2).fetchMetrics("router-1"))
                .assertNext(samples -> assertThat(samples).containsExactly(
                        new MetricSample("cpu_load", 42.5, Instant.parse("2026-10-20T12:00:00Z")),
                        new MetricSample("door_open", 1.0, Instant.parse("2026-10-20T12:00:00Z"))))
                .verifyComplete();
    }

    @Test
    void retriesServerErrorsThenSucceeds() {
        server.createContext("/devices/olt-1/metrics", exchange -> {
            int n = requests.incrementAndGet();
            byte[] body = (n < 3 ? "oops" : "[{\"metric\":\"rx_power\",\"value\":-21.3}]")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(n < 3 ? 503 : 200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        StepVerifier.create(client(2).fetchMetrics("olt-1"))
                .assertNext(samples -> assertThat(samples).singleElement()
                        .satisfies(s -> assertThat(s.value()).isEqualTo(-21.3)))
                .verifyComplete();
        assertThat(requests).hasValue(3);
    }

    @Test
    void doesNotRetryClientErrors() {
        respond("/devices/unknown/metrics", 404, "not found");

        StepVerifier.create(client(2).fetchMetrics("unknown"))
                .expectError(WebClientResponseException.NotFound.class)
                .verify();
        assertThat(requests).hasValue(1);
    }

    @Test
    void failsWhenDeviceDoesNotAnswerInTime() {
        server.createContext("/devices/lte-1/metrics", exchange -> {
            requests.incrementAndGet();
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });

        StepVerifier.create(client(1).fetchMetrics("lte-1"))
                .expectError()
                .verify(Duration.ofSeconds(5));
        assertThat(requests).hasValue(2);
    }

    private void respond(String path, int status, String body) {
        server.createContext(path, exchange -> {
            requests.incrementAndGet();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
    }

    private EmulatorClient client(int retries) {
        PollerProperties props = new PollerProperties(
                new PollerProperties.Backend("http://unused", Duration.ofMinutes(1), Duration.ofSeconds(1)),
                new PollerProperties.Emulator("http://127.0.0.1:" + server.getAddress().getPort(),
                        Duration.ofMillis(500), Duration.ofMillis(200), retries, Duration.ofMillis(10)),
                new PollerProperties.Poll(Duration.ofSeconds(30), 4),
                new PollerProperties.Kafka("nms.metrics", 1));
        WebClient webClient = new WebClientConfig().emulatorWebClient(WebClient.builder(), props);
        return new EmulatorClient(webClient, props);
    }
}
