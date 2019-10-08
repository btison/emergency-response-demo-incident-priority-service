package com.redhat.cajun.navy.incident.priority;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.healthchecks.HealthCheckHandler;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.micrometer.PrometheusScrapingHandler;

public class RestApiVerticle extends AbstractVerticle {

    @Override
    public Completable rxStart() {
        return initializeHttpServer(config());
    }

    private Completable initializeHttpServer(JsonObject config) {

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
}
