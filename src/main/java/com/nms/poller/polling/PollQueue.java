package com.nms.poller.polling;

import com.nms.poller.config.PollerProperties;
import com.nms.poller.registry.Device;
import com.nms.poller.registry.MetricTypeRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

@Component
public class PollQueue {

    private record Key(long deviceId, String metric) {
    }

    private record Task(Key key, Instant dueAt) {
    }

    private final PollerProperties.Poll props;
    private final MetricTypeRegistry metricTypes;
    private final PriorityQueue<Task> queue = new PriorityQueue<>(Comparator.comparing(Task::dueAt));
    private final Map<Key, Task> tasks = new HashMap<>();
    private final Map<Long, Device> devices = new HashMap<>();

    public PollQueue(PollerProperties props, MetricTypeRegistry metricTypes) {
        this.props = props.poll();
        this.metricTypes = metricTypes;
    }

    public synchronized void sync(Collection<Device> current, Instant now) {
        Map<Long, Device> byId = new LinkedHashMap<>();
        current.forEach(d -> byId.put(d.id(), d));

        devices.keySet().removeIf(id -> !byId.containsKey(id));
        tasks.keySet().removeIf(key -> !byId.containsKey(key.deviceId()));

        byId.forEach((id, device) -> {
            boolean known = devices.containsKey(id);
            devices.put(id, device);
            if (!known) {
                schedule(new Key(id, null), now);
            }
        });
    }

    public synchronized List<PollRequest> pollDue(Instant now) {
        Map<Long, Set<String>> due = new LinkedHashMap<>();
        while (!queue.isEmpty() && !queue.peek().dueAt().isAfter(now)) {
            Task task = queue.poll();
            if (tasks.get(task.key()) != task) {
                continue;
            }
            Key key = task.key();
            Set<String> metrics = due.computeIfAbsent(key.deviceId(), id -> new LinkedHashSet<>());
            if (key.metric() == null) {
                schedule(key, now.plus(props.defaultInterval()));
            } else {
                metrics.add(key.metric());
                schedule(key, nextSlot(task.dueAt(), metricTypes.intervalFor(key.metric()), now));
            }
        }
        List<PollRequest> requests = new ArrayList<>();
        due.forEach((id, metrics) -> requests.add(new PollRequest(devices.get(id), Set.copyOf(metrics))));
        return requests;
    }

    public synchronized void discovered(long deviceId, Collection<String> metrics, Instant now) {
        if (!devices.containsKey(deviceId) || metrics.isEmpty()) {
            return;
        }
        tasks.remove(new Key(deviceId, null));
        for (String metric : metrics) {
            Key key = new Key(deviceId, metric);
            if (!tasks.containsKey(key)) {
                schedule(key, now.plus(metricTypes.intervalFor(metric)));
            }
        }
    }

    public synchronized int size() {
        return tasks.size();
    }

    private void schedule(Key key, Instant dueAt) {
        Task task = new Task(key, dueAt);
        tasks.put(key, task);
        queue.add(task);
    }

    private static Instant nextSlot(Instant dueAt, Duration interval, Instant now) {
        long slots = Duration.between(dueAt, now).dividedBy(interval) + 1;
        return dueAt.plus(interval.multipliedBy(slots));
    }
}
