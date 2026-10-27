# NMS-poller

Приложение 2 мини-NMS (см. `system-overview.md`, `diagram.md`): по расписанию получает реестр
устройств у NMS Backend, опрашивает Device Emulator по REST и публикует события метрик в Kafka.

```
DeviceRegistry ──GET /api/devices──► NMS Backend         (кэш, обновление раз в 60s)
PollingScheduler ─► DevicePoller ─► EmulatorClient ──GET /devices/{id}/metrics──► Device Emulator
                                 └► MetricProducer ──key=deviceId──► Kafka: nms.metrics
```

## Поведение

- **Реестр устройств** (`registry`): периодический `GET /api/devices`. Если backend недоступен,
  опрос продолжается по последнему известному списку. Устройства с `enabled=false` не опрашиваются.
- **Опрос** (`polling`, `emulator`): каждые `poll.interval` все устройства опрашиваются параллельно
  (не более `poll.concurrency`). Каждая попытка ограничена `emulator.timeout`; таймауты, сетевые
  ошибки и 5xx повторяются `emulator.retries` раз с экспоненциальной задержкой, 4xx — нет.
- **События** (`kafka`): одно событие на каждую метрику с `pollStatus=SUCCESS`. Если устройство
  не ответило после всех ретраев — одно событие `pollStatus=TIMEOUT` с `metricType=null`,
  `value=null`. Boolean-значения («дверь открыта», «220В есть») приводятся к `1.0` / `0.0`.
  Если эмулятор не прислал `timestamp`, используется время опроса.

```json
{"deviceId":"router-1","metricType":"cpu_load","value":87.5,"timestamp":"2026-10-20T12:00:00Z","pollStatus":"SUCCESS"}
{"deviceId":"lte-1","metricType":null,"value":null,"timestamp":"2026-10-20T12:00:30Z","pollStatus":"TIMEOUT"}
```

## Запуск

```bash
./mvnw verify                 # сборка + тесты
./mvnw spring-boot:run        # нужен Kafka на localhost:9092
docker build -t nms-poller .
```

| Переменная окружения | По умолчанию |
|---|---|
| `NMS_BACKEND_URL` | `http://localhost:8080` |
| `DEVICE_EMULATOR_URL` | `http://localhost:8081` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `SERVER_PORT` | `8082` (`/actuator/health`) |

Остальные настройки — `src/main/resources/application.yml`, префикс `nms.poller`.
