package com.redhat.emergency.response.incident.priority;

import java.util.HashMap;
import java.util.Map;

import com.redhat.emergency.response.incident.priority.tracing.TracingKafkaConsumer;
import com.redhat.emergency.response.incident.priority.tracing.TracingUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private TracingKafkaConsumer<String, String> kafkaConsumer;

    private Counter kafkaMessageCounter;

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

            MeterRegistry registry = BackendRegistries.getDefaultNow();
            kafkaMessageCounter = Counter.builder("kafka-message-consumed").tag("topic",config().getString("topic-incident-assignment-event"))
                    .tag("consumergroup",config().getString("group-id")).register(registry);

            future.complete();
        }));
    }

    private void handleMessage(KafkaConsumerRecord<String, String> msg) {
        kafkaMessageCounter.increment();
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
                Span span = TracingUtils.buildChildSpan("incidentAssignmentEvent", msg, tracer);
                try {
                    String incidentId = message.getJsonObject("body").getString("incidentId");
                    log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                            + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());
                    DeliveryOptions options = new DeliveryOptions();
                    TracingUtils.injectInEventBusMessage(span.context(), options, tracer);
                    vertx.eventBus().send("incident-assignment-event", body, options);
                } finally {
                    span.finish();
                }
            } else if ("IncidentReportedEvent".equals(messageType)) {
                Span span = TracingUtils.buildChildSpan("incidentReportedEvent", msg, tracer);
                try {
                    JsonObject body = message.getJsonObject("body");
                    JsonObject payload = new JsonObject()
                            .put("incidentId", body.getString("id"))
                            .put("assigment", false)
                            .put("lat", body.getFloat("lat").toString())
                            .put("lon", body.getFloat("lon").toString());

                    String incidentId = payload.getString("incidentId");
                    log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Topic: " + msg.topic()
                            + " ,  partition: " + msg.partition() + ", offset: " + msg.offset());

                    DeliveryOptions options = new DeliveryOptions();
                    TracingUtils.injectInEventBusMessage(span.context(), options, tracer);
                    vertx.eventBus().send("incident-assignment-event", payload, options);
                } finally {
                    span.finish();
                }
            } else if ("PriorityZoneClearEvent".equals(messageType)) {
                vertx.eventBus().send("priority-zone-clear-event", "");
            } else {
                log.debug("Unexpected message type '" + messageType + "' in message " + message + ". Ignoring message");
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
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
        }
        return Completable.complete();
    }


}
