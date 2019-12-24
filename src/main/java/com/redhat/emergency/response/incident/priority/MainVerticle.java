package com.redhat.emergency.response.incident.priority;

import java.util.Optional;

import com.redhat.emergency.response.incident.priority.ha.BootstrapVerticle;
import io.reactivex.Completable;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {

    private boolean runningOnKubernetes = false;

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

        String namespace = System.getenv("KUBERNETES_NAMESPACE");
        if (namespace != null) {
            //we're running in Kubernetes
            runningOnKubernetes = true;
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
                    JsonObject ha = json.getJsonObject("ha");
                    ha.put("running-on-kubernetes", runningOnKubernetes);
                    ha.put("namespace", Optional.ofNullable(namespace).orElse(""));
                    return vertx.rxDeployVerticle(BootstrapVerticle::new, new DeploymentOptions().setConfig(ha)).ignoreElement()
                            .andThen(vertx.rxDeployVerticle(MessageConsumerVerticle::new, new DeploymentOptions().setConfig(kafka)).ignoreElement())
                            .andThen(vertx.rxDeployVerticle(RulesVerticle::new, new DeploymentOptions()).ignoreElement())
                            .andThen(vertx.rxDeployVerticle(RestApiVerticle::new, new DeploymentOptions().setConfig(http)).ignoreElement());
                });
    }

}
