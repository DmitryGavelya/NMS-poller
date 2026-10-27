package com.nms.poller.registry;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * HTTP-клиент реестра устройств NMS Backend.
 */
@Component
public class DeviceRegistryClient {

    private static final ParameterizedTypeReference<List<Device>> DEVICE_LIST = new ParameterizedTypeReference<>() {
    };

    private final WebClient backendWebClient;

    public DeviceRegistryClient(@Qualifier("backendWebClient") WebClient backendWebClient) {
        this.backendWebClient = backendWebClient;
    }

    public Mono<List<Device>> fetchDevices() {
        return backendWebClient.get()
                .uri("/api/devices")
                .retrieve()
                .bodyToMono(DEVICE_LIST)
                .defaultIfEmpty(List.of());
    }
}
