package com.nms.poller.kafka;

public enum PollStatus {
    /** Устройство ответило, значение метрики снято. */
    SUCCESS,
    /** Устройство не ответило: таймаут или ошибка после всех ретраев. */
    TIMEOUT
}
