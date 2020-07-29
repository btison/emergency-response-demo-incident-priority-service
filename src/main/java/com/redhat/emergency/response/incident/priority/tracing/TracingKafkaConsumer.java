package com.redhat.emergency.response.incident.priority.tracing;

import java.util.Map;

import io.opentracing.Tracer;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.reactivex.core.streams.Pipe;
import io.vertx.reactivex.core.streams.WriteStream;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class TracingKafkaConsumer<K, V> implements io.vertx.reactivex.core.streams.ReadStream<io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord<K, V>> {

    private KafkaConsumer<K, V> delegate;

    private Tracer tracer;

    private TracingKafkaConsumer(KafkaConsumer<K, V> consumer, Tracer tracer) {
        this.delegate = consumer;
        this.tracer = tracer;
    }

    public static <K, V> TracingKafkaConsumer<K, V> create(Vertx vertx, Map<String, String> config, Tracer tracer) {
        KafkaConsumer<K, V> ret = KafkaConsumer.newInstance(io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx.getDelegate(), config), io.vertx.lang.rx.TypeArg.unknown(), io.vertx.lang.rx.TypeArg.unknown());
        return new TracingKafkaConsumer<>(ret, tracer);
    }

    @Override
    public ReadStream getDelegate() {
        return delegate.getDelegate();
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    public TracingKafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler) {

        Handler<KafkaConsumerRecord<K, V>> decorated = event -> {
            TracingUtils.buildAndFinishChildSpan(event, tracer);
            handler.handle(event);
        };
        delegate.handler(decorated);
        return this;
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> pause() {
        delegate.pause();
        return this;
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> resume() {
        delegate.resume();
        return this;
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> fetch(long amount) {
        delegate.fetch(amount);
        return this;
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public Pipe<KafkaConsumerRecord<K, V>> pipe() {
        return delegate.pipe();
    }

    @Override
    public void pipeTo(WriteStream<KafkaConsumerRecord<K, V>> dst) {
        delegate.pipeTo(dst);
    }

    @Override
    public void pipeTo(WriteStream<KafkaConsumerRecord<K, V>> dst, Handler<AsyncResult<Void>> handler) {
        delegate.pipeTo(dst, handler);
    }

    @Override
    public Observable<KafkaConsumerRecord<K, V>> toObservable() {
        return delegate.toObservable();
    }

    @Override
    public Flowable<KafkaConsumerRecord<K, V>> toFlowable() {
        return delegate.toFlowable();
    }

    public TracingKafkaConsumer<K, V> subscribe(String topic) {
        delegate.subscribe(topic);
        return this;
    }

    public void commit() {
        delegate.commit();
    }

    public TracingKafkaConsumer<K, V> unsubscribe() {
        delegate.unsubscribe();
        return this;
    }

    public void close() {
        delegate.close();
    }

}
