package com.redhat.emergency.response.incident.priority;

import java.util.HashMap;
import java.util.Map;

import com.redhat.cajun.navy.incident.priority.tracing.TracingKafkaConsumer;
import com.redhat.cajun.navy.incident.priority.tracing.TracingKafkaUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private TracingKafkaConsumer<String, String> kafkaConsumer;

    private Tracer tracer;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            tracer = GlobalTracer.get();
            Map<String, String> kafkaConfig = new HashMap<>();
            kafkaConfig.put("bootstrap.servers", config().getString("bootstrap-servers"));
            kafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConfig.put("group.id", config().getString("group-id"));
            kafkaConfig.put("auto.offset.reset", "earliest");
            kafkaConfig.put("enable.auto.commit", "false");
            kafkaConsumer = TracingKafkaConsumer.<String, String>create(vertx, kafkaConfig, tracer)
                .handler(this::handleMessage)
                .subscribe(config().getString("topic-incident-assignment-event"));
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

            Span span = TracingKafkaUtils.buildChildSpan("incidentAssignmentEvent", msg, tracer);
            try {
                String incidentId = message.getJsonObject("body").getString("incidentId");
                log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                        + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());

                DeliveryOptions options = new DeliveryOptions();
                TracingKafkaUtils.injectInEventBusMessage(span.context(), options, tracer);
                vertx.eventBus().send("incident-assignment-event", body, options);
            } finally {
                span.finish();
            }

        } finally {
            //commit message
            kafkaConsumer.commit();
        }
    }

    @Override
    public Completable rxStop() {
        if (kafkaConsumer != null) {
            kafkaConsumer.commit();
            kafkaConsumer.unsubscribe().close();
        }
        return Completable.complete();
    }


}
