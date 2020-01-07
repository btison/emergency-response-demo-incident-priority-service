package com.redhat.emergency.response.incident.priority;

import java.time.Instant;
import java.util.UUID;

import com.redhat.emergency.response.incident.priority.ha.MessageConsumerVerticle;
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
        JsonObject message = new JsonObject()
                .put("id", UUID.randomUUID().toString()).put("messageType", MessageConsumerVerticle.COMMAND_EVENT)
                .put("invokingService","IncidentPriorityService").put("timestamp", Instant.now().toEpochMilli())
                .put("body", new JsonObject().put("command", "reset"));
        vertx.eventBus().rxRequest("command-message-producer", message)
                .subscribe((json) -> rc.response().setStatusCode(200).end(), rc::fail);
    }

    private <T> void statusReady(Message<T> message) {
        ready = true;
        message.reply(new JsonObject());
    }
}
