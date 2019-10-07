package com.redhat.cajun.navy.incident.priority.tracing;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.reactivex.core.streams.Pipe;
import io.vertx.reactivex.core.streams.WriteStream;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class TracingKafkaConsumer<K, V> implements io.vertx.reactivex.core.streams.ReadStream<io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord<K, V>> {

    private KafkaConsumer<K, V> delegate;

    @Override
    public ReadStream getDelegate() {
        return delegate.getDelegate();
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> exceptionHandler(Handler<Throwable> handler) {
        return delegate.exceptionHandler(handler);
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> handler(Handler<KafkaConsumerRecord<K, V>> handler) {
        return delegate.handler(handler);
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> pause() {
        return delegate.pause();
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> resume() {
        return delegate.resume();
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> fetch(long amount) {
        return delegate.fetch(amount);
    }

    @Override
    public io.vertx.reactivex.core.streams.ReadStream<KafkaConsumerRecord<K, V>> endHandler(Handler<Void> endHandler) {
        return delegate.endHandler(endHandler);
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
}
