package com.redhat.emergency.response.incident.priority.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.impl.AsyncResultSingle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class ConsumerUtils {

    public static Single<JsonObject> getLastEvent(String topic, Vertx vertx, Map<String, String> kafkaConfig, Long pollTimeout) {
        KafkaConsumer<String, String> kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
        try {
            return Single.create(emitter -> kafkaConsumer.rxPartitionsFor(topic).subscribe(partitionInfos -> {
                Set<TopicPartition> partitions = partitionInfos.stream()
                        .map(p -> new TopicPartition(p.getTopic(), p.getPartition()))
                        .collect(Collectors.toSet());
                kafkaConsumer.rxAssign(partitions)
                        .andThen(rxEndOffsets(kafkaConsumer, partitions))
                        .subscribe(topicPartitionLongMap -> {
                            Long lastOffset = 0L;
                            for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap.entrySet()) {
                                lastOffset = entry.getValue();
                            }
                            if (lastOffset == 0L) {
                                lastOffset = 1L;
                            }
                            Long finalLastOffset = lastOffset;
                            kafkaConsumer.rxAssignment().subscribe(topicPartitions -> {
                                List<Completable> cList = new ArrayList<>();
                                topicPartitions.forEach(topicPartition ->
                                        cList.add(kafkaConsumer.rxSeek(topicPartition, finalLastOffset - 1)));
                                Completable.concat(cList).andThen(kafkaConsumer.rxPoll(pollTimeout))
                                        .subscribe(records -> {
                                            JsonObject result = new JsonObject();
                                            if (!records.isEmpty()) {
                                                KafkaConsumerRecord<String, String> last = records.recordAt(records.size() - 1);
                                                JsonObject msg = new JsonObject(last.value());
                                                result.put("id", msg.getString("id"));
                                                result.put("offset", last.offset());
                                            }
                                            emitter.onSuccess(result);
                                        }, emitter::onError);
                            }, emitter::onError);
                        }, emitter::onError);
            }, emitter::onError));
        } finally {
            kafkaConsumer.close();
        }
    }

    private static <T,V> Single<Map<TopicPartition, Long>> rxEndOffsets(KafkaConsumer<T,V> kafkaConsumer, Set<TopicPartition> partitions) {
        return AsyncResultSingle.toSingle(asyncResultHandler -> kafkaConsumer.getDelegate().endOffsets(partitions, asyncResultHandler));
    }



}
