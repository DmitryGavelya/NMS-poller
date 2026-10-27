package com.nms.poller.registry;

import com.nms.poller.config.PollerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class DeviceRegistry {

    private static final Logger log = LoggerFactory.getLogger(DeviceRegistry.class);

    private final DeviceRegistryClient client;
    private final PollerProperties props;
    private final AtomicReference<List<Device>> devices = new AtomicReference<>(List.of());

    public DeviceRegistry(DeviceRegistryClient client, PollerProperties props) {
        this.client = client;
        this.props = props;
    }

    public List<Device> devices() {
        return devices.get();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initialLoad() {
        refresh();
    }

    @Scheduled(fixedDelayString = "${nms.poller.backend.registry-refresh-interval}",
            initialDelayString = "${nms.poller.backend.registry-refresh-interval}")
    public void refresh() {
        try {
            List<Device> fetched = client.fetchDevices()
                    .block(props.backend().timeout().multipliedBy(2));
            List<Device> pollable = fetched == null ? List.of()
                    : fetched.stream().filter(Device::isPollable).toList();
            List<Device> previous = devices.getAndSet(pollable);
            if (previous.size() != pollable.size()) {
                log.info("Device registry updated: {} -> {} pollable devices", previous.size(), pollable.size());
            }
        } catch (Exception e) {
            log.warn("Failed to refresh device registry, keeping {} cached devices: {}",
                    devices.get().size(), e.toString());
        }
    }
}
