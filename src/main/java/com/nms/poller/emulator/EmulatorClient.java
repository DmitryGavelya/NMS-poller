package com.nms.poller.emulator;

import com.nms.poller.config.PollerProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

@Component
public class EmulatorClient {

    private static final ParameterizedTypeReference<List<MetricSample>> SAMPLE_LIST = new ParameterizedTypeReference<>() {
    };

    private final WebClient emulatorWebClient;
    private final PollerProperties.Emulator props;

    public EmulatorClient(@Qualifier("emulatorWebClient") WebClient emulatorWebClient, PollerProperties props) {
        this.emulatorWebClient = emulatorWebClient;
        this.props = props.emulator();
    }

    public Mono<List<MetricSample>> fetchMetrics(String deviceId) {
        return fetchMetrics(deviceId, Set.of());
    }

    public Mono<List<MetricSample>> fetchMetrics(String deviceId, Set<String> metrics) {
        return emulatorWebClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.path("/devices/{id}/metrics");
                    if (!metrics.isEmpty()) {
                        uriBuilder.queryParam("metric", metrics);
                    }
                    return uriBuilder.build(deviceId);
                })
                .retrieve()
                .bodyToMono(SAMPLE_LIST)
                .defaultIfEmpty(List.of())
                .map(samples -> metrics.isEmpty() ? samples
                        : samples.stream().filter(s -> metrics.contains(s.metric())).toList())
                .timeout(props.timeout())
                .retryWhen(Retry.backoff(props.retries(), props.retryBackoff())
                        .filter(EmulatorClient::isRetryable)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
    }

    static boolean isRetryable(Throwable t) {
        if (t instanceof WebClientResponseException e) {
            return e.getStatusCode().is5xxServerError();
        }
        return t instanceof TimeoutException || t instanceof WebClientRequestException;
    }
}
