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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private KafkaConsumer<String, String> kafkaConsumer;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config().getString("bootstrap-servers"));
            kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, config().getString("group-id"));
            kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            kafkaConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config().getString("security-protocol"));
            kafkaConfig.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, config().getString("ssl-keystore-type"));
            kafkaConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config().getString("ssl-keystore-location"));
            kafkaConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config().getString("ssl-keystore-password"));
            kafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, config().getString("ssl-truststore-type"));
            kafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config().getString("ssl-truststore-location"));
            kafkaConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config().getString("ssl-truststore-password"));
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
