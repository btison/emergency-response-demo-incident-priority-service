package com.redhat.emergency.response.incident.priority.ha.infra.election;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.shareddata.LocalMap;

public class GlobalState {

    private static volatile Vertx vertx;

    private static volatile boolean initialized = false;

    public static void init(Vertx v) {
        if (initialized) {
            return;
        }
        vertx = v;
        LocalMap<String, Boolean> globalState = getLocalMap();
        globalState.put("nodeReady", Boolean.FALSE);
        globalState.put("nodeLive", Boolean.TRUE);
        globalState.put("canBecomeLeader", Boolean.TRUE);
        initialized = true;
    }

    private static LocalMap<String, Boolean> getLocalMap() {
        return vertx.sharedData().getLocalMap("GlobalState");
    }

    public static boolean isNodeReady() {
        return getLocalMap().get("nodeReady");
    }

    public static void setNodeReady(boolean nodeReady) {
        getLocalMap().put("nodeReady", nodeReady);
    }

    public static boolean isNodeLive() {
        return getLocalMap().get("nodeLive");
    }

    public static void setNodeLive(boolean nodeLive) {
        getLocalMap().put("nodeReady", nodeLive);
    }

    public static boolean isCanBecomeLeader() {
        return getLocalMap().get("canBecomeLeader");
    }

    public static void setCanBecomeLeader(boolean canBecomeLeader) {
        getLocalMap().put("canBecomeLeader", canBecomeLeader);
    }

}
