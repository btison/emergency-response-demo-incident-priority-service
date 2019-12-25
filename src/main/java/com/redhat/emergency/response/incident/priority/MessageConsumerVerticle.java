package com.redhat.emergency.response.incident.priority;

import java.util.HashMap;
import java.util.Map;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private KafkaConsumer<String, String> kafkaConsumer;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put("bootstrap.servers", config().getString("bootstrap-servers"));
            kafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put("group.id", config().getString("group-id"));
            kafkaConfig.put("auto.offset.reset", "earliest");
            kafkaConfig.put("enable.auto.commit", "false");
            kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            kafkaConsumer.handler(this::handleMessage);
            kafkaConsumer.subscribe(config().getString("topic-incident-assignment-event"));
            future.complete();
        }));
    }

    private void handleMessage(KafkaConsumerRecord<String, String> msg) {

        try {
            JsonObject message = new JsonObject(msg.value());

            if (message.isEmpty()) {
                log.warn("Message " + msg.key() + " has no contents. Ignoring message");
                return;
            }
            String messageType = message.getString("messageType");
            if (!("IncidentAssignmentEvent".equals(messageType))) {
                log.debug("Unexpected message type '" + messageType + "' in message " + message + ". Ignoring message");
                return;
            }
            JsonObject body = message.getJsonObject("body");
            if (body == null
                    || body.getString("incidentId") == null
                    || body.getBoolean("assignment") == null) {
                log.warn("Message of type '" + "' has unexpected structure: " + message.toString());
            }
            String incidentId = message.getJsonObject("body").getString("incidentId");
            log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                    + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());

            vertx.eventBus().send("incident-assignment-event", body);

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
