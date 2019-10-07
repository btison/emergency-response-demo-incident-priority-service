package com.redhat.emergency.response.incident.priority;

import io.jaegertracing.Configuration;
import io.opentracing.util.GlobalTracer;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.impl.AsyncResultSingle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private static Logger log = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public Completable rxStart() {

        ConfigStoreOptions localStore = new ConfigStoreOptions().setType("file").setFormat("yaml")
                .setConfig(new JsonObject()
                        .put("path", System.getProperty("vertx-config-path")));
        ConfigStoreOptions configMapStore = new ConfigStoreOptions()
                .setType("configmap")
                .setFormat("yaml")
                .setConfig(new JsonObject()
                        .put("name", System.getProperty("application.configmap", "incident-priority-service"))
                        .put("key", System.getProperty("application.configmap.key", "application-config.yaml")));
        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
        if (System.getenv("KUBERNETES_NAMESPACE") != null) {
            //we're running in Kubernetes
            options.addStore(configMapStore);
        } else {
            //default to local config
            options.addStore(localStore);
        }
        
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
        return retriever.rxGetConfig()
                .flatMapCompletable(json -> {
                    JsonObject http = json.getJsonObject("http");
                    JsonObject kafka = json.getJsonObject("kafka");
                    return rxInitTracer(json.getJsonObject("tracing")).ignoreElement()
                            .andThen(vertx.rxDeployVerticle(MessageConsumerVerticle::new, new DeploymentOptions().setConfig(kafka)).ignoreElement())
                            .andThen(vertx.rxDeployVerticle(RulesVerticle::new, new DeploymentOptions()).ignoreElement())
                            .andThen(vertx.rxDeployVerticle(RestApiVerticle::new, new DeploymentOptions().setConfig(http)).ignoreElement());
                });
    }

    private Single<Void> rxInitTracer(JsonObject config) {
        return AsyncResultSingle.toSingle(handler -> initTracer(config, handler));
    }

    private void initTracer(JsonObject config, Handler<AsyncResult<Void>> completionHandler) {
        String serviceName = config.getString("service-name");
        if (serviceName == null || serviceName.isEmpty()) {
            log.warn("No Service Name set. Skipping initialization of the Jaeger Tracer.");
            reportResult(completionHandler, Future.succeededFuture());
            return;
        }

        Configuration configuration = new Configuration(serviceName)
                .withSampler(new Configuration.SamplerConfiguration()
                        .withType(config.getString("sampler-type"))
                        .withParam(getPropertyAsNumber(config, "sampler-param"))
                        .withManagerHostPort(config.getString("sampler-manager-host-port")))
                .withReporter(new Configuration.ReporterConfiguration()
                        .withLogSpans(config.getBoolean("reporter-log-spans"))
                        .withFlushInterval(config.getInteger("reporter-flush-interval"))
                        .withMaxQueueSize(config.getInteger("max-queue-size"))
                        .withSender(new Configuration.SenderConfiguration()
                                .withAgentHost(config.getString("agent-host"))
                                .withAgentPort(config.getInteger("agent-port"))));
        GlobalTracer.registerIfAbsent(configuration.getTracer());
        reportResult(completionHandler, Future.succeededFuture());
    }

    private void reportResult(Handler<AsyncResult<Void>> completionHandler, AsyncResult<Void> result) {
        if (completionHandler != null) {
            completionHandler.handle(result);
        }
    }

    private Number getPropertyAsNumber(JsonObject json, String key) {
        Object o  = json.getValue(key);
        if (o instanceof Number) {
            return (Number) o;
        }
        return null;
    }
}
