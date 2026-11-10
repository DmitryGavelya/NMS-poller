package com.nms.poller.registry;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
public class MetricTypeRegistryClient {

    private static final ParameterizedTypeReference<List<MetricType>> METRIC_TYPE_LIST = new ParameterizedTypeReference<>() {};

    private final WebClient backendWebClient;

    public MetricTypeRegistryClient(@Qualifier("backendWebClient") WebClient backendWebClient) {
        this.backendWebClient = backendWebClient;
    }

    public Mono<List<MetricType>> fetchMetricTypes() {
        return backendWebClient.get()
                .uri("/api/metric-types")
                .retrieve()
                .bodyToMono(METRIC_TYPE_LIST)
                .defaultIfEmpty(List.of());
    }
}
