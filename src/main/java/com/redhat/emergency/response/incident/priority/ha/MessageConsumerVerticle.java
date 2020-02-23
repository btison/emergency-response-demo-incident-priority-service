package com.redhat.emergency.response.incident.priority.ha;

import java.util.HashMap;
import java.util.Map;

import io.reactivex.Completable;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(MessageConsumerVerticle.class);

    private KafkaConsumer<String, String> kafkaConsumer;

    private String eventTopic;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> kafkaConfig = kafkaConfig();
            kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            eventTopic = config().getString("topic-incident-priority-event");
            kafkaConsumer.handler(this::handleMessage);
            kafkaConsumer.subscribe(eventTopic);
            future.complete();
            log.info("MessageConsumerVerticle deployed");
        }));
    }

    @Override
    public Completable rxStop() {
        log.info("Stopping MessageConsumerVerticle");
        return kafkaConsumer.rxClose();
    }

    private Map<String, String> kafkaConfig() {
        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config().getString("bootstrap-servers"));
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, config().getString("group-id"));
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return kafkaConfig;
    }

    private void handleMessage(KafkaConsumerRecord<String, String> msg) {
        log.debug("Consumed message with key '" + msg.key() + "'. Topic: " + msg.topic()
                + " ,  partition: " + msg.partition() + ", offset: " + msg.offset() + ". Message: " + msg.value());
        kafkaConsumer.commit();
    }


}
