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
            vertx.eventBus().consumer("control-message-producer", this::handleMessage);
            future.complete();
            log.info("MessageProducerVerticle deployed");
        }));
    }

    private void handleMessage(Message<JsonObject> message) {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(controlTopic, message.body().getString("id"), message.body().toString());
        kafkaProducer.rxWrite(record).subscribe(
                () -> {
                    log.debug("Sent message to topic " + controlTopic + ". Message : " + message.body().toString());
                    message.reply(new JsonObject());
                },
                t -> {
                    log.error("Error sending message to topic " + controlTopic, t);
                    message.reply(new JsonObject());
                });
    }


}
