package com.nms.poller.config;

import io.netty.channel.ChannelOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient backendWebClient(WebClient.Builder builder, PollerProperties props) {
        PollerProperties.Backend backend = props.backend();
        return builder.clone()
                .baseUrl(backend.baseUrl())
                .clientConnector(connector(backend.timeout(), backend.timeout()))
                .build();
    }

    @Bean
    public WebClient emulatorWebClient(WebClient.Builder builder, PollerProperties props) {
        PollerProperties.Emulator emulator = props.emulator();
        return builder.clone()
                .baseUrl(emulator.baseUrl())
                .clientConnector(connector(emulator.connectTimeout(), emulator.timeout()))
                .build();
    }

    private static ReactorClientHttpConnector connector(Duration connectTimeout, Duration responseTimeout) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis())
                .responseTimeout(responseTimeout);
        return new ReactorClientHttpConnector(httpClient);
    }
}
