package com.redhat.emergency.response.incident.priority.ha;

import java.util.HashMap;
import java.util.Map;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

public class MessageProducerVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(MessageProducerVerticle.class);

    private KafkaProducer<String, String> kafkaProducer;

    private String controlTopic;

    private String eventTopic;

    @Override
    public Completable rxStart() {
        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config().getString("bootstrap-servers"));
            kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            kafkaProducer = KafkaProducer.create(vertx, kafkaConfig);
            controlTopic = config().getString("topic-incident-priority-control");
            eventTopic = config().getString("topic-incident-priority-event");
            vertx.eventBus().consumer("control-message-producer", this::handleControlMessage);
            vertx.eventBus().consumer("command-message-producer", this::handleCommandMessage);
            future.complete();
            log.info("MessageProducerVerticle deployed");
        }));
    }

    private void handleControlMessage(Message<JsonObject> message) {
        handleMessage(message, controlTopic, message.body().getString("id"));
    }

    private void handleCommandMessage(Message<JsonObject> message) {
        handleMessage(message, eventTopic, message.body().getString("id"));
    }

    private void handleMessage(Message<JsonObject> message, String topic, String key) {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topic, key, message.body().toString());
        kafkaProducer.rxWrite(record).subscribe(
                () -> {
                    log.debug("Sent message to topic " + topic + ". Message : " + message.body().toString());
                    message.reply(new JsonObject());
                },
                t -> {
                    log.error("Error sending message to topic " + topic, t);
                    message.fail(-1, "Error sending message to topic " + topic);
                });
    }


}
