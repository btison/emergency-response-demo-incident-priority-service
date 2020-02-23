package com.redhat.emergency.response.incident.priority;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.oauth2.OAuth2FlowType;
import io.vertx.ext.auth.oauth2.impl.OAuth2TokenImpl;
import io.vertx.ext.healthchecks.Status;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.auth.oauth2.OAuth2Auth;
import io.vertx.reactivex.ext.auth.oauth2.providers.KeycloakAuth;
import io.vertx.reactivex.ext.healthchecks.HealthCheckHandler;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import io.vertx.reactivex.ext.web.handler.OAuth2AuthHandler;
import io.vertx.reactivex.micrometer.PrometheusScrapingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(RestApiVerticle.class);

    @Override
    public Completable rxStart() {
        return initializeHttpServer(config());
    }

    private Completable initializeHttpServer(JsonObject config) {

        Router router = Router.router(vertx);

        router.route("/metrics").handler(PrometheusScrapingHandler.create());
        router.route().handler(BodyHandler.create());

        //configure KeyCloak
        JsonObject keycloakJson = new JsonObject()
            .put("realm", config.getString("REALM"))
            .put("auth-server-url", config.getString("AUTH_URL"))
            .put("ssl-required", "external")
            .put("resource", config.getString("VERTX_CLIENTID"))
            .put("credentials", new JsonObject().put("secret", config.getString("VERTX_CLIENT_SECRET")))
            .put("confidential-port", 0);
        OAuth2Auth oauth2 = KeycloakAuth.create(vertx, OAuth2FlowType.AUTH_CODE, keycloakJson);
        OAuth2AuthHandler oauth2Handler = OAuth2AuthHandler.create(oauth2);
        oauth2Handler.setupCallback(router.get("/callback"));

        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx)
                .register("health", f -> f.complete(Status.OK()));
        router.get("/health").handler(healthCheckHandler);
        router.get("/priority/:incidentId").handler(this::priority);
        router.post("/reset").handler(this::reset);

        // list of priority zones can be accessed anonymously
        router.get("/priority-zones").handler(this::getPriorityZones);

        // other priority-zone endpoints require auth with incident_commander role
        router.route("/priority-zone*").handler(oauth2Handler).handler(this::incidentCommanderHandler);
        router.post("/priority-zone/:id").handler(this::applyPriorityZone);
        router.delete("/priority-zones").handler(this::clearPriorityZones);

        return vertx.createHttpServer()
                .requestHandler(router)
                .rxListen(config.getJsonObject("http").getInteger("port", 8080))
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

    /**
     * Return a 403 for this request if the associated SSO user (if applicable) doesn't have the incident_commander role
     * 
     * @param rc the RoutingContext associated with this request
     */
    private void incidentCommanderHandler(RoutingContext rc) {
        OAuth2TokenImpl user = (OAuth2TokenImpl) rc.user().getDelegate();
        user.setTrustJWT(true);
        user.isAuthorized("realm:incident_commander", result -> {
            if ( ! result.result()) {
                log.error("Unauthorized access to resource {} by user {}", rc.request().path(), user.accessToken().getString("preferred_username"));
                rc.response().setStatusCode(403).end();
            } else {
                rc.next();
            }
        });
    }

    /**
     * Return a JsonArray containing all PriorityZones in working memory
     * 
     * @param rc the RoutingContext associated with this request
     */
    private void getPriorityZones(RoutingContext rc) {
        vertx.eventBus().rxRequest("priority-zones", new JsonObject())
        .subscribe((json) -> rc.response().setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(json.body().toString()),
                rc::fail);
    }

    /**
     * Create or update the priority zone identified by the given id
     * 
     * @param rc the RoutingContext associated with this request
     */
    private void applyPriorityZone(RoutingContext rc) {
        String id = rc.request().getParam("id");

        JsonObject body = rc.getBodyAsJson();
        if (body == null
                || body.getString("lat") == null
                || body.getString("lon") == null
                || body.getString("radius") == null) {
            rc.response().setStatusCode(400).end("Missing parameters. Expected [lat:String, lon:String, radius:String]");
        }

        body.put("id", id);

        log.debug("Received priority zone application request: {}", body.encodePrettily());

        vertx.eventBus().send("priority-zone-application-event", body);
        rc.response().setStatusCode(200).end();
    }

    /**
     * Delete all priority zones from working memory
     * 
     * @param rc the RoutingContext associated with this request
     */
    private void clearPriorityZones(RoutingContext rc) {
        vertx.eventBus().send("priority-zone-clear-event", "");
        rc.response().setStatusCode(204).end();
    }

    private void reset(RoutingContext rc) {
        vertx.eventBus().rxRequest("reset", new JsonObject())
                .subscribe((json) -> rc.response().setStatusCode(200).end(), rc::fail);
    }
}
