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
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private KafkaConsumer<String, String> kafkaConsumer;

    private KafkaProducer<String, String> kafkaProducer;

    private String eventTopic;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> consumerKafkaConfig = new HashMap<>();
            consumerKafkaConfig.put("bootstrap.servers", config().getString("bootstrap-servers"));
            consumerKafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerKafkaConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerKafkaConfig.put("group.id", config().getString("group-id"));
            consumerKafkaConfig.put("auto.offset.reset", "earliest");
            consumerKafkaConfig.put("enable.auto.commit", "false");
            kafkaConsumer = KafkaConsumer.create(vertx, consumerKafkaConfig);
            kafkaConsumer.handler(this::handleMessage);
            kafkaConsumer.subscribe(config().getString("topic-incident-assignment-event"));

            Map<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config().getString("bootstrap-servers"));
            kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            kafkaProducer = KafkaProducer.create(vertx, kafkaConfig);
            eventTopic = config().getString("topic-incident-priority-event");

            future.complete();
        }));
    }

    private void handleMessage(KafkaConsumerRecord<String, String> msg) {
        // filter out messages. IncidentReportedEvent and IncidentAssignmentEvent are forwarded to the event queue
        try {
            JsonObject message = new JsonObject(msg.value());

            if (message.isEmpty()) {
                log.warn("Message " + msg.key() + " has no contents. Ignoring message");
                return;
            }
            String messageType = message.getString("messageType");
            if ("IncidentAssignmentEvent".equals(messageType)) {
                JsonObject body = message.getJsonObject("body");
                if (body == null
                        || body.getString("incidentId") == null
                        || body.getBoolean("assignment") == null
                        || body.getString("lat") == null
                        || body.getString("lon") == null) {
                    log.warn("Message of type '" + "' has unexpected structure: " + message.toString());
                }
                String incidentId = message.getJsonObject("body").getString("incidentId");
                log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                        + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());
                forwardMessage(msg.key(), msg.value());
            } else if ("IncidentReportedEvent".equals(messageType)) {
                JsonObject body = message.getJsonObject("body");
                JsonObject payload = new JsonObject()
                    .put("incidentId", body.getString("id"))
                    .put("assigment", false)
                    .put("lat", body.getFloat("lat").toString())
                    .put("lon", body.getFloat("lon").toString());
                
                String incidentId = payload.getString("incidentId");
                log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                        + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());
                forwardMessage(msg.key(), msg.value());
            } else {
                log.debug("Unexpected message type '" + messageType + "' in message " + message + ". Ignoring message");
            }

        } finally {
            //commit message
            kafkaConsumer.commit();
        }
    }

    private void forwardMessage(String key, String message) {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(eventTopic, key, message);
        kafkaProducer.rxWrite(record).subscribe(
                () -> log.debug("Sent message to topic " + eventTopic + ". Message : " + message),
                t -> log.error("Error sending message to topic " + eventTopic, t));
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
