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
import java.util.concurrent.TimeoutException;

/**
 * Опрос устройства в эмуляторе: {@code GET /devices/{id}/metrics}.
 * Каждая попытка ограничена таймаутом; сетевые ошибки, таймауты и 5xx повторяются
 * с экспоненциальной задержкой. 4xx не повторяются — это ошибка запроса, а не сбой устройства.
 */
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
        return emulatorWebClient.get()
                .uri("/devices/{id}/metrics", deviceId)
                .retrieve()
                .bodyToMono(SAMPLE_LIST)
                .defaultIfEmpty(List.of())
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
