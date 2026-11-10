package com.nms.poller.emulator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BooleanSample.class, name = "BOOLEAN", names = {"boolean", "bool"}),
        @JsonSubTypes.Type(value = DoubleSample.class, name = "DOUBLE", names = {"double", "number"}),
        @JsonSubTypes.Type(value = StringSample.class, name = "STRING", names = {"string"})
})
public sealed interface MetricSample permits BooleanSample, DoubleSample, StringSample {

    String metric();

    Object value();

    Instant timestamp();
}
