package com.redhat.emergency.response.incident.priority.launcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.redhat.emergency.response.incident.priority.MainVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

    private static final Logger log = LoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) {
        new Launcher().launch();
    }

    private void launch() {

        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setJvmMetricsEnabled(true)
                .setEnabled(true));

        Vertx vertx = Vertx.vertx(options);
        addShutdownHook(vertx);
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploy(new MainVerticle(), vertx, deploymentOptions, res -> {
            if (res.failed()) {
                vertx.close();
            }
        });
    }

    private void deploy(Verticle verticle, Vertx vertx, DeploymentOptions options, Handler<AsyncResult<String>> completionHandler) {
        vertx.deployVerticle(verticle, options, createHandler(completionHandler));
    }

    private Handler<AsyncResult<String>> createHandler(final Handler<AsyncResult<String>> completionHandler) {
        return res -> {
            if (res.failed()) {
                Throwable cause = res.cause();
                log.error("Failed in deploying verticle", cause);
                if (cause instanceof VertxException) {
                    VertxException ve = (VertxException) cause;
                    log.error(ve.getMessage());
                    if (ve.getCause() != null) {
                        log.error(ve.getCause().getMessage(), ve.getCause());
                    }
                }
            } else {
                log.info("Succeeded in deploying verticle");
            }
            if (completionHandler != null) {
                completionHandler.handle(res);
            }
        };
    }

    private static void addShutdownHook(Vertx vertx) {
        Runtime.getRuntime().addShutdownHook(new Thread(getTerminationRunnable(vertx)));
    }

    public static Runnable getTerminationRunnable(Vertx vertx) {
        return () -> {
            CountDownLatch latch = new CountDownLatch(1);
            if (vertx != null) {
                vertx.close(ar -> {
                    if (!ar.succeeded()) {
                        log.error("Failure in stopping Vert.x", ar.cause());
                    }
                    latch.countDown();
                });
                try {
                    if (!latch.await(2, TimeUnit.MINUTES)) {
                        log.error("Timed out waiting to undeploy all");
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

}
