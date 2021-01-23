package com.redhat.emergency.response.incident.priority;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private static final String INCIDENT_ASSIGNMENT_EVENT = "IncidentAssignmentEvent";
    private static final String INCIDENT_REPORTED_EVENT = "IncidentReportedEvent";
    private static final String PRIORITY_ZONE_CLEAR_EVENT = "PriorityZoneClearEvent";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {INCIDENT_ASSIGNMENT_EVENT, INCIDENT_REPORTED_EVENT, PRIORITY_ZONE_CLEAR_EVENT};

    private KafkaConsumer<String, CloudEvent> kafkaConsumer;

    private Counter kafkaMessageCounter;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put("bootstrap.servers", config().getString("bootstrap-servers"));
            kafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put("value.deserializer", "com.redhat.emergency.response.incident.priority.cloudevents.CloudEventDeserializer");
            kafkaConfig.put("group.id", config().getString("group-id"));
            kafkaConfig.put("auto.offset.reset", "earliest");
            kafkaConfig.put("enable.auto.commit", "false");
            kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            kafkaConsumer.handler(this::handleMessage);
            kafkaConsumer.subscribe(config().getString("topic-incident-assignment-event"));

            MeterRegistry registry = BackendRegistries.getDefaultNow();
            kafkaMessageCounter = Counter.builder("kafka-message-consumed").tag("topic",config().getString("topic-incident-assignment-event"))
                    .tag("consumergroup",config().getString("group-id")).register(registry);

            future.complete();
        }));
    }

    private void handleMessage(KafkaConsumerRecord<String, CloudEvent> msg) {
        kafkaMessageCounter.increment();
        try {
            CloudEvent cloudEvent = msg.value();
            if (cloudEvent == null) {
                log.warn("Message is not a CloudEvent. Message is ignored");
                return;
            }
            String contentType = cloudEvent.getDataContentType();
            if (contentType == null || !(contentType.equalsIgnoreCase("application/json"))) {
                log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
                return;
            }
            String messageType = cloudEvent.getType();
            if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType))) {
                log.debug("CloudEvent with type '" + messageType + "' is ignored");
                return;
            }
            if (cloudEvent.getData() == null || cloudEvent.getData().toBytes().length == 0) {
                log.warn("Message " + msg.key() + " has no contents. Ignoring message");
                return;
            }
            JsonObject event = new JsonObject(new String(cloudEvent.getData().toBytes()));
            if (INCIDENT_ASSIGNMENT_EVENT.equals(messageType)) {
                if (event.getString("incidentId") == null || event.getBoolean("assignment") == null ||
                        event.getString("lat") == null || event.getString("lon") == null) {
                    log.warn("Message of type '" + "' has unexpected structure: " + event.toString());
                    return;
                }
                String incidentId = event.getString("incidentId");
                log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                        + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());

                vertx.eventBus().send("incident-assignment-event", event);
            } else if (INCIDENT_REPORTED_EVENT.equals(messageType)) {
                if (event.getString("incidentId") == null ||
                        event.getDouble("lat") == null || event.getDouble("lon") == null) {
                    log.warn("Message of type '" + "' has unexpected structure: " + event.toString());
                    return;
                }
                JsonObject payload = new JsonObject()
                        .put("incidentId", event.getString("id"))
                        .put("assigment", false)
                        .put("lat", event.getDouble("lat").toString())
                        .put("lon", event.getDouble("lon").toString());

                String incidentId = payload.getString("incidentId");
                log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                        + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());

                vertx.eventBus().send("incident-assignment-event", payload);
            } else if (PRIORITY_ZONE_CLEAR_EVENT.equals(messageType)) {
                vertx.eventBus().send("priority-zone-clear-event", "");
            }
        } catch (Exception e) {
            log.error("Error processing CloudEvent", e);
        } finally {
            //commit message
            kafkaConsumer.commit();
        }
    }

    @Override
    public Completable rxStop() {
        if (kafkaConsumer != null) {
            kafkaConsumer.commit();
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
        }
        return Completable.complete();
    }


}
