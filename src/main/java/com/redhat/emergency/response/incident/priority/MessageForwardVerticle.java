package com.redhat.emergency.response.incident.priority;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

public class MessageForwardVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(MessageForwardVerticle.class);

    private String sourceTopic;

    private String eventTopic;

    private KafkaStreams streams;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, config().getString("streams-group-id"));
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config().getString("bootstrap-servers"));
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            sourceTopic = config().getString("topic-incident-assignment-event");
            eventTopic = config().getString("topic-incident-priority-event");

            streams = new KafkaStreams(builder().build(), props);
            streams.start();

            future.complete();
        }));
    }

    private StreamsBuilder builder() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .map((KeyValueMapper<String, String, KeyValue<String, JsonObject>>) (key, value) -> {
                    JsonObject message = new JsonObject(value);
                    return new KeyValue<>(key, message);
                }).filter((key, value) -> {
                    if (value.isEmpty()) {
                        log.warn("Message " + key + " has no contents. Ignoring message");
                        return false;
                    }
                    String messageType = value.getString("messageType");
                    if ("IncidentAssignmentEvent".equals(messageType)) {
                        String incidentId = value.getJsonObject("body").getString("incidentId");
                        log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Message: " + value);
                        return true;
                    } else if ("IncidentReportedEvent".equals(messageType)) {
                        String incidentId = value.getJsonObject("body").getString("id");
                        log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'. Message: "+ value);
                        return true;
                    } else {
                        log.debug("Unexpected message type '" + messageType + "' in message " + value + ". Ignoring message");
                    }
                    return false;
                }).map((KeyValueMapper<String, JsonObject, KeyValue<String, String>>) (key, value) -> {
                    String messageType = value.getString("messageType");
                    if ("IncidentAssignmentEvent".equals(messageType)) {
                        return new KeyValue<>(value.getString("id"), value.encode());
                    } else {
                        JsonObject body = value.getJsonObject("body");
                        Object lat = body.getValue("lat");
                        Object lon = body.getValue("lon");

                        JsonObject message = new JsonObject()
                                .put("id", UUID.randomUUID().toString()).put("messageType", "IncidentAssignmentEvent")
                                .put("invokingService", "IncidentPriorityService").put("timestamp", Instant.now().toEpochMilli())
                                .put("body", new JsonObject()
                                    .put("incidentId", body.getString("id"))
                                    .put("assignment", false)
                                    .put("lat", lat instanceof String? lat : lat.toString())
                                    .put("lon", lon instanceof String? lon : lon.toString()));
                        return new KeyValue<>(value.getString("id"), message.encode());
                    }
                }).to(eventTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    @Override
    public Completable rxStop() {
        if (streams != null) {
            streams.close();
        }
        return Completable.complete();
    }


}
