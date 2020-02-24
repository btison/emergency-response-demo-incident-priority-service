package com.redhat.emergency.response.incident.priority.ha;

import com.redhat.emergency.response.incident.priority.ha.infra.election.GlobalState;
import com.redhat.emergency.response.incident.priority.ha.infra.election.LeaderElectionVerticle;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(BootstrapVerticle.class);

    private CoreKube coreKube;

    @Override
    public Completable rxStart() {

        GlobalState.init(vertx);
        Single<String> single;
        if (config().getBoolean("running-on-kubernetes")) {
            coreKube = new CoreKube(config().getString("leader-configmap"));
            single = vertx.rxDeployVerticle(new LeaderElectionVerticle(coreKube.getKubernetesClient(), coreKube.getConfiguration(), null));
        } else {
            single = Single.just("");
        }
        single.subscribe(id -> {
            if (config().getBoolean("running-on-kubernetes")) {
                leaderElection();
            }
            GlobalState.setNodeReady(Boolean.TRUE);
        }, err -> log.error("Failed deploying LeaderElection verticle", err));
        return Completable.complete();
    }



    @Override
    public Completable rxStop() {
        return Completable.complete();
    }

    private void leaderElection() {
        // send message to LeaderElectionVerticle
        vertx.eventBus().send("leader-election-start", new JsonObject());
    }
}
