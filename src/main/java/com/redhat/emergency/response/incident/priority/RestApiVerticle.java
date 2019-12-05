package com.redhat.emergency.response.incident.priority;

import java.util.Random;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.healthchecks.HealthCheckHandler;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import io.vertx.reactivex.micrometer.PrometheusScrapingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(RestApiVerticle.class);

    private boolean injectFault = false;
    private int errorCode = 200;
    private int percentage = 100;
    private int delayMilliSeconds = 0;


    @Override
    public Completable rxStart() {
        return initializeHttpServer(config()).andThen(initializeFaultInjectorHttpServer(config()));
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

    private Completable initializeFaultInjectorHttpServer(JsonObject config) {
        Router router = Router.router(vertx);
        router.post("/reset").handler(this::resetFault);
        router.route("/inject").handler(BodyHandler.create());
        router.post("/inject").handler(this::injectFault);
        return vertx.createHttpServer()
                .requestHandler(router)
                .rxListen(9080)
                .ignoreElement();
    }

    private void priority(RoutingContext rc) {
        log.info("Incoming Request");
        boolean inject = false;
        if (injectFault && percentage > 0) {
            Random random = new Random();
            int r = random.nextInt(100) + 1;
            if (r <= percentage) {
                inject = true;
            }
        }
        if (inject && delayMilliSeconds == 0 && errorCode >= 500) {
            log.info("Returning error code " + errorCode);
            rc.fail(errorCode);
            return;
        }

        String incidentId = rc.request().getParam("incidentId");

        if (inject && delayMilliSeconds > 0) {
            vertx.setTimer(delayMilliSeconds, event -> {
                if (errorCode >= 500) {
                    log.info("Returning error code " + errorCode);
                    rc.fail(errorCode);
                } else {
                    log.info("Returning http code " + 200);
                    vertx.eventBus().rxRequest("incident-priority", new JsonObject().put("incidentId", incidentId))
                            .subscribe((json) -> rc.response().setStatusCode(200)
                                            .putHeader("content-type", "application/json")
                                            .end(json.body().toString()),
                                    rc::fail);
                }
            });
        } else {
            log.info("Returning http code " + 200);
            vertx.eventBus().rxRequest("incident-priority", new JsonObject().put("incidentId", incidentId))
                    .subscribe((json) -> rc.response().setStatusCode(200)
                                    .putHeader("content-type", "application/json")
                                    .end(json.body().toString()),
                            rc::fail);
        }
    }

    private void injectFault(RoutingContext rc) {
        JsonObject inject = rc.getBodyAsJson();
        log.info("Inject Fault method called with payload " + inject.toString());
        injectFault = true;
        if (inject.getInteger("error") != null) {
            this.errorCode = inject.getInteger("error");
        }
        if (inject.getInteger("delay") != null) {
            int delay = inject.getInteger("delay");
            if (delay < 0) {
                delay = 0;
            }
            this.delayMilliSeconds = delay;
        }
        if (inject.getInteger("percentage") != null) {
            int p = inject.getInteger("percentage");
            if (p < 0) p = 0;
            if (p > 100) p = 100;
            percentage = p;
        }
        rc.response().setStatusCode(201).end();
    }

    private void resetFault(RoutingContext rc) {
        injectFault = false;
        errorCode = 200;
        percentage = 100;
        delayMilliSeconds = 0;
        rc.response().setStatusCode(201).end();
    }

    private void reset(RoutingContext rc) {
        vertx.eventBus().rxRequest("reset", new JsonObject())
                .subscribe((json) -> rc.response().setStatusCode(200).end(), rc::fail);
    }
}
