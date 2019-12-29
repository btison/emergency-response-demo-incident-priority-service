package com.redhat.emergency.response.incident.priority;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.healthchecks.HealthCheckHandler;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.micrometer.PrometheusScrapingHandler;

public class RestApiVerticle extends AbstractVerticle {

    private boolean ready = false;

    @Override
    public Completable rxStart() {
        vertx.eventBus().consumer("instance-status-ready", this::statusReady);
        return initializeHttpServer(config());
    }

    private Completable initializeHttpServer(JsonObject config) {

        Router router = Router.router(vertx);

        router.route("/metrics").handler(PrometheusScrapingHandler.create());

        HealthCheckHandler livenessHandler = HealthCheckHandler.create(vertx)
                .register("liveness", f -> f.complete(Status.OK()));
        HealthCheckHandler readinessHandler = HealthCheckHandler.create(vertx)
                .register("readiness", f -> {
                    if (!ready) {
                        f.complete(Status.KO());
                    } else {
                        f.complete(Status.OK());
                    }
                });
        router.get("/liveness").handler(livenessHandler);
        router.get("/readiness").handler(readinessHandler);
        router.get("/priority/:incidentId").handler(this::priority);
        router.post("/reset").handler(this::reset);

        return vertx.createHttpServer()
                .requestHandler(router)
                .rxListen(config.getInteger("port", 8080))
                .ignoreElement();
    }

    private void priority(RoutingContext rc) {
        String incidentId = rc.request().getParam("incidentId");
        vertx.eventBus().rxRequest("incident-priority", new JsonObject().put("incidentId", incidentId))
                .subscribe((json) -> rc.response().setStatusCode(200)
                                .putHeader("content-type", "application/json")
                                .end(json.body().toString()),
                        rc::fail);
    }

    private void reset(RoutingContext rc) {
        vertx.eventBus().rxRequest("reset", new JsonObject())
                .subscribe((json) -> rc.response().setStatusCode(200).end(), rc::fail);
    }

    private <T> void statusReady(Message<T> tMessage) {
        ready = true;
    }
}
