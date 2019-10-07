package com.redhat.emergency.response.incident.priority;

import com.redhat.cajun.navy.incident.priority.tracing.TracingKafkaUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.healthchecks.HealthCheckHandler;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.micrometer.PrometheusScrapingHandler;

public class RestApiVerticle extends AbstractVerticle {

    private Tracer tracer;

    @Override
    public Completable rxStart() {
        return initializeHttpServer(config());
    }

    private Completable initializeHttpServer(JsonObject config) {

        tracer = GlobalTracer.get();

        Router router = Router.router(vertx);

        router.route("/metrics").handler(PrometheusScrapingHandler.create());

        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx)
                .register("health", f -> f.complete(Status.OK()));
        router.get("/health").handler(healthCheckHandler);
        router.get("/priority/:incidentId").handler(this::priority);
        router.post("/reset").handler(this::reset);

        return vertx.createHttpServer()
                .requestHandler(router)
                .rxListen(config.getInteger("port", 8080))
                .ignoreElement();
    }

    private void priority(RoutingContext rc) {

        Span span = TracingKafkaUtils.buildChildSpan(rc.request(), tracer);

        String incidentId = rc.request().getParam("incidentId");
        DeliveryOptions options = new DeliveryOptions();
        TracingKafkaUtils.injectInEventBusMessage(span.context(), options, tracer);
        vertx.eventBus().rxRequest("incident-priority", new JsonObject().put("incidentId", incidentId), options)
            .subscribe(json -> {
                Tags.HTTP_STATUS.set(span, rc.response().getStatusCode());
                rc.response().setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(json.body().toString());
                span.finish();
            }, throwable -> {
                Tags.ERROR.set(span, Boolean.TRUE);
                Tags.HTTP_STATUS.set(span, 500);
                rc.fail(500);
                span.finish();
            });
    }

    private void reset(RoutingContext rc) {
        vertx.eventBus().rxRequest("reset", new JsonObject())
                .subscribe((json) -> rc.response().setStatusCode(200).end(), rc::fail);
    }
}
