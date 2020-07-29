package com.redhat.emergency.response.incident.priority.tracing;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.http.HttpServerRequest;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class TracingUtils {

    private static final String COMPONENT = "incident-priority-service";

    public static <K, V> void buildAndFinishChildSpan(KafkaConsumerRecord<K, V> record, Tracer tracer) {
        io.opentracing.contrib.kafka.TracingKafkaUtils.buildAndFinishChildSpan(record.getDelegate().record(), tracer);
    }

    public static <K, V> Span buildChildSpan(String operationName, KafkaConsumerRecord<K, V> record, Tracer tracer) {
        SpanContext parentContext = io.opentracing.contrib.kafka.TracingKafkaUtils.extractSpanContext(record.getDelegate().record().headers(), tracer);

        Tracer.SpanBuilder spanBuilder = spanBuilder(operationName, tracer);
        if (parentContext != null) {
            spanBuilder.addReference(References.CHILD_OF, parentContext);
        }
        return spanBuilder.start();
    }

    public static Span buildChildSpan(String operationName, Message<JsonObject> msg, Tracer tracer) {
        Tracer.SpanBuilder spanBuilder = spanBuilder(operationName, "kiesession", tracer);

        SpanContext parentContext = extractSpanContext(msg, tracer);
        if (parentContext != null) {
            spanBuilder.addReference(References.CHILD_OF, parentContext);
        }
        return spanBuilder.start();
    }

    public static Span buildChildSpan(HttpServerRequest request, Tracer tracer) {

        SpanContext parentContext = extractSpanContext(request, tracer);

        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(request.method().name())
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                .withTag(Tags.COMPONENT, "vertx-web-request")
                .withTag(Tags.HTTP_METHOD, request.method().name())
                .withTag(Tags.HTTP_URL, request.uri());
        if (parentContext != null) {
            spanBuilder.addReference(References.CHILD_OF, parentContext);
        }

        return spanBuilder.start();
    }

    public static void injectInEventBusMessage(SpanContext spanContext, DeliveryOptions options, Tracer tracer) {
        tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new DeliveryOptionsInjectAdapter(options));
    }

    private static Tracer.SpanBuilder spanBuilder(String operationName, Tracer tracer) {
        return spanBuilder(operationName, Tags.SPAN_KIND_CONSUMER, tracer);
    }

    private static Tracer.SpanBuilder spanBuilder(String operationName, String spanKind, Tracer tracer) {
        return tracer.buildSpan(operationName).ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), spanKind)
                .withTag(Tags.COMPONENT.getKey(), COMPONENT);
    }

    private static SpanContext extractSpanContext(Message<JsonObject> msg, Tracer tracer) {
        return tracer.extract(Format.Builtin.TEXT_MAP, new MultiMapExtractAdapter(msg.getDelegate().headers()));
    }

    private static SpanContext extractSpanContext(HttpServerRequest request, Tracer tracer) {
        return tracer.extract(Format.Builtin.HTTP_HEADERS, new MultiMapExtractAdapter(request.getDelegate().headers()));
    }
}
