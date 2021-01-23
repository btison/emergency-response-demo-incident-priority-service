package com.redhat.emergency.response.incident.priority.cloudevents;

import java.util.Map;

import io.cloudevents.CloudEvent;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudEventDeserializer implements Deserializer<CloudEvent> {

    private static Logger log = LoggerFactory.getLogger(CloudEventDeserializer.class);

    private io.cloudevents.kafka.CloudEventDeserializer delegate;

    public CloudEventDeserializer() {
        delegate = new io.cloudevents.kafka.CloudEventDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public CloudEvent deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public CloudEvent deserialize(String topic, Headers headers, byte[] data) {
        try {
            return delegate.deserialize(topic, headers, data);
        } catch (Exception e) {
            log.error("Error deserializing CloudEvent", e);
        }
        return null;
    }

}
