package com.redhat.emergency.response.incident.priority.ha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.redhat.emergency.response.incident.priority.ha.infra.election.State;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(MessageConsumerVerticle.class);

    private State currentState = State.REPLICA;

    private KafkaConsumer<String, String> kafkaConsumer;

    private KafkaConsumer<String, String> kafkaSecondaryConsumer;

    private String eventTopic;

    private String controlTopic;

    private long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;

    private long pollTimeout;

    private volatile String processingKey = "";

    private boolean started = false;

    private boolean ready = false;

    private boolean ignoreFirstMessageAfterStateChange = false;

    private volatile PolledTopic polledTopic = PolledTopic.CONTROL;

    @Override
    public Completable rxStart() {

        return Completable.fromMaybe(vertx.rxExecuteBlocking(future -> {
            Map<String, String> kafkaConfig = kafkaConfig();
            kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            if (currentState.equals(State.REPLICA)) {
                kafkaSecondaryConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            }
            eventTopic = config().getString("topic-incident-assignment-event");
            controlTopic = config().getString("topic-incident-priority-control");
            pollTimeout = config().getLong("poll-timeout", 1000L);
            vertx.eventBus().consumer("message-consumer-update-state", this::updateStatus);
            future.complete();
            log.info("MessageConsumerVerticle deployed");
        }));
    }

    @Override
    public Completable rxStop() {
        log.info("Stopping MessageConsumerVerticle");
        started = false;
        if (kafkaSecondaryConsumer != null) {
            return kafkaSecondaryConsumer.rxClose().andThen(kafkaConsumer.rxClose());
        } else {
            return kafkaConsumer.rxClose();
        }
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

    private void updateStatus(Message<JsonObject> message) {
        State state = State.valueOf(message.body().getString("state"));
        boolean changedState = !state.equals(currentState);
        log.debug("Processing update status. State = " + state.name() + ", state changed = " + changedState);
        if(currentState == null || changedState) {
            currentState = state;
        }
        if (started && changedState && !currentState.equals(State.BECOMING_LEADER)) {
            updateOnRunningConsumer(state);
        } else if (!started) {
            started = true;
            if (state.equals(State.REPLICA)) {
                // TODO ask and wait for a snapshot before starting
            }
            //State.BECOMING_LEADER won't start consuming
            if (state.equals(State.LEADER) || state.equals(State.REPLICA)) {
                log.info("Calling enableConsume with state {}", state);
                enableConsume(state);
            }
        }
    }

    private void updateOnRunningConsumer(State state) {
        log.info("Update on running consumer. State = " + state);
        ignoreFirstMessageAfterStateChange = true;
        stopConsume();
        Completable c = restartConsumers(state);
        c.subscribe(() -> enableConsume(state));
    }

    private void stopConsume() {
        kafkaConsumer.pause();
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.pause();
        }
    }

    private Completable restartConsumers(State state) {
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.unsubscribe();
            kafkaSecondaryConsumer.close();
        }
        return Completable.fromMaybe(vertx.rxExecuteBlocking(f -> {
            Map<String, String> kafkaConfig = kafkaConfig();
            kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            if (state.equals(State.REPLICA)) {
                kafkaSecondaryConsumer = KafkaConsumer.create(vertx, kafkaConfig);
            }
            f.complete();
        }));
    }

    private void enableConsume(State state) {
        if (state.equals(State.LEADER)) {
            currentState = State.LEADER;
        } else if (state.equals(State.REPLICA) ) {
            currentState = State.REPLICA;
            if (kafkaSecondaryConsumer == null) {
                kafkaSecondaryConsumer = KafkaConsumer.create(vertx, kafkaConfig());
            }
        }
        setLastProcessedKey().subscribe(this::assignAndStartConsume);
    }

    private void assignAndStartConsume() {
        Completable c = currentState.equals(State.LEADER) ? assignAsALeader() : assignReplica();
        c.subscribe(this::consumeMessages);
    }

    protected Completable assignAsALeader() {
        return assignConsumer(kafkaConsumer, eventTopic);
    }

    protected Completable assignReplica() {
        return assignConsumer(kafkaConsumer, eventTopic)
                .andThen(assignConsumer(kafkaSecondaryConsumer, controlTopic));
    }

    protected Completable assignConsumer(KafkaConsumer<String, String> kafkaConsumer, String topic) {
        return Completable.create(emitter -> kafkaConsumer.rxPartitionsFor(topic).subscribe(partitionInfos -> {
            Set<TopicPartition> partitions = partitionInfos.stream()
                    .map(p -> new TopicPartition(p.getTopic(), p.getPartition()))
                    .collect(Collectors.toSet());
            kafkaConsumer.rxAssign(partitions).subscribe(() -> {
                // TODO snapshotinfos
                if (currentState.equals(State.LEADER)) {
                    kafkaConsumer.rxAssignment().subscribe(topicPartitions -> {
                        List<Completable> cList = new ArrayList<>();
                        topicPartitions.forEach(topicPartition -> cList.add(kafkaConsumer.rxSeek(topicPartition, lastProcessedEventOffset)));
                        Completable.concat(cList).subscribe(emitter::onComplete);
                    }, emitter::onError);
                } else if (currentState.equals(State.REPLICA)) {
                    kafkaConsumer.rxAssignment().subscribe(topicPartitions -> {
                        List<Completable> cList = new ArrayList<>();
                        topicPartitions.forEach(topicPartition -> cList.add(kafkaConsumer.rxSeek(topicPartition, lastProcessedControlOffset)));
                        Completable.concat(cList).subscribe(emitter::onComplete);
                    }, emitter::onError);
                }
            }, emitter::onError);
        }, emitter::onError));
    }

    private void consumeMessages() {
        if (currentState.equals(State.LEADER)) {
            defaultProcessAsLeader();
        } else {
            defaultProcessAsAReplica();
        }
    }

    private void defaultProcessAsLeader() {
        markInstanceReady().subscribe(() -> kafkaConsumer.handler(this::processLeader));
    }

    private void defaultProcessAsAReplica() {
        log.debug("Process messages as replica. Processing key = " + processingKey + ", processing key offset = " + processingKeyOffset);
        if (processingKey == null) {
            markInstanceReady().subscribe(this::setReplicaKafkaConsumerHandlers);
        }
        else {
            setReplicaKafkaConsumerHandlers();
        }
    }

    private void setReplicaKafkaConsumerHandlers() {
        if (polledTopic.equals(PolledTopic.CONTROL)) {
            kafkaConsumer.pause();
        } else {
            kafkaSecondaryConsumer.pause();
        }
        kafkaSecondaryConsumer.handler(this::processControlAsReplica);
        kafkaConsumer.handler(this::processEventAsReplica);
    }

    private void processLeader(KafkaConsumerRecord<String, String> record) {
        if (!started) {
            return;
        }
        log.debug("Processing event record as Leader. Offset: " + record.offset() + ", key: " + processingKey);
        // TODO snapshot handling
        if (ignoreFirstMessageAfterStateChange) {
            log.debug("Ignoring first message after state change");
            ignoreFirstMessageAfterStateChange = false;
            saveOffset(record, kafkaConsumer);
        } else {
            // pause consumption of new kafka messages until assignment is handled
            kafkaConsumer.pause();
            handleMessage(record).andThen(sendControlMessage(record.key())).subscribe(() -> {
                processingKey = record.key();
                processingKeyOffset = record.offset();
                saveOffset(record, kafkaConsumer);
                kafkaConsumer.resume();
            });
        }
    }

    private void processControlAsReplica(KafkaConsumerRecord<String, String> record) {
        if (!started) {
            return;
        }
        log.debug("Processing control record as Replica. Offset: " + record.offset() + ", key: " + record.key());
        if (record.offset() == processingKeyOffset + 1 || processingKey == null) {
            lastProcessedControlOffset = record.offset();
            processingKey = record.key();
            processingKeyOffset = record.offset();
            pollEvents();
            log.debug("Switch to consume events");
        }
        else if (record.offset() == processingKeyOffset) {
            pollEvents();
            log.debug("Switch to consume events");
        }
        saveOffset(record, kafkaSecondaryConsumer);
    }

    private void processEventAsReplica(KafkaConsumerRecord<String, String> record) {
        if (!started) {
            return;
        }
        log.debug("Processing event record as Replica. Offset: " + record.offset() + ", key: " + record.key());
        // pause consumption of new kafka messages until assignment is handled
        kafkaConsumer.pause();
        handleMessage(record).subscribe(() -> {
            lastProcessedEventOffset = record.offset();
            if (record.key().equals(processingKey)) {
                markInstanceReady().subscribe(() -> {
                    pollControl();
                    log.debug("Switch to consume control messages");
                    saveOffset(record, kafkaConsumer);
                });
            } else {
                saveOffset(record, kafkaConsumer);
                kafkaConsumer.resume();
            }
        });
    }

    protected void saveOffset(KafkaConsumerRecord<String, String> record, KafkaConsumer<String, String> kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() +1, ""));
        kafkaConsumer.getDelegate().commit(map);
    }

    private Completable handleMessage(KafkaConsumerRecord<String, String> record) {

        return Completable.create(emitter -> {
            JsonObject message = new JsonObject(record.value());

            if (message.isEmpty()) {
                log.warn("Message " + record.key() + " has no contents. Ignoring message");
                emitter.onComplete();
                return;
            }
            String messageType = message.getString("messageType");
            if (!("IncidentAssignmentEvent".equals(messageType))) {
                log.debug("Unexpected message type '" + messageType + "' in message " + message + ". Ignoring message");
                emitter.onComplete();
                return;
            }
            JsonObject body = message.getJsonObject("body");
            if (body == null
                    || body.getString("incidentId") == null
                    || body.getBoolean("assignment") == null) {
                log.warn("Message of type '" + "' has unexpected structure: " + message.toString());
                emitter.onComplete();
                return;
            }
            String incidentId = message.getJsonObject("body").getString("incidentId");
            log.debug("Consumed '" + messageType + "' message for incident '" + incidentId + "'.");

            vertx.eventBus().rxRequest("incident-assignment-event", body)
                .subscribe(o -> emitter.onComplete(), t -> {
                    log.error("Exception while sending assignment event", t);
                    emitter.onComplete();
                });
        });

    }

    private Completable sendControlMessage(String id) {
        return Completable.create(emitter -> vertx.eventBus().rxRequest("control-message-producer", new JsonObject().put("id", id))
            .subscribe(o -> emitter.onComplete(), t -> {
                log.error("Exception while sending control message", t);
                emitter.onComplete();
            }));
    }

    private Completable setLastProcessedKey() {
        return Completable.create(emitter -> ConsumerUtils.getLastEvent(controlTopic, vertx, kafkaConfig(), pollTimeout).subscribe((result, throwable) -> {
            if (throwable != null) {
                log.error("Exception when fetching last processed key", throwable);
            } else {
                processingKey = result.getString("id");
                processingKeyOffset = result.getLong("offset") == null? 0L : result.getLong("offset");
                log.debug("Set last processed control key: key = " + processingKey + ", offset = " + processingKeyOffset);
            }
            emitter.onComplete();
        }));
    }

    private Completable markInstanceReady() {
        if (!ready) {
            ready = true;
            log.info("Instance is ready. State = " + currentState + ", Processing key = " + processingKey + ", Processing offset = " + processingKeyOffset);
            return Completable.create(emitter -> vertx.eventBus().rxRequest("instance-status-ready", new JsonObject())
                    .subscribe(objectMessage -> emitter.onComplete(), throwable -> {
                        log.error("Exception while marking instance ready", throwable);
                        emitter.onComplete();
                    }));
        } else {
            return Completable.complete();
        }
    }

    protected void pollControl(){
        polledTopic = PolledTopic.CONTROL;
        kafkaConsumer.pause();
        kafkaSecondaryConsumer.resume();
    }

    protected void pollEvents(){
        polledTopic = PolledTopic.EVENTS;
        kafkaSecondaryConsumer.pause();
        kafkaConsumer.resume();
    }

    public enum PolledTopic {
        EVENTS, CONTROL
    }
}
