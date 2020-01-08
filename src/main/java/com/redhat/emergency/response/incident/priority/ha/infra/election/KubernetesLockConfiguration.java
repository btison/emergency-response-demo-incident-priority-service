package com.redhat.emergency.response.incident.priority.ha.infra.election;

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.json.JsonObject;

public class KubernetesLockConfiguration {

    public static final String DEFAULT_LEADER_ELECTION_CONFIGMAP = "incident-priority-service-leaders";
    public static final double DEFAULT_JITTER_FACTOR = 1.2;
    public static final long DEFAULT_LEASE_DURATION_MILLIS = 5000;
    public static final long DEFAULT_RETRY_PERIOD_MILLIS = 5000;
    public static final String DEFAULT_GROUP_NAME = "incident-priority";

    /**
     * Name of the current pod (defaults to host name).
     */
    private String podName;

    /**
     * Name of the ConfigMap used for locking.
     */
    private String configMapName;

    /**
     * Name of the lock group (or namespace according to the Camel cluster convention) within the chosen ConfigMap.
     */
    private String groupName;

    /**
     * Labels used to identify the members of the cluster.
     */
    private Map<String, String> clusterLabels = new HashMap<>();

    /**
     * A jitter factor to apply in order to prevent all pods to call Kubernetes APIs in the same instant.
     */
    private double jitterFactor;

    /**
     * The default duration of the lease for the current leader.
     */
    private long leaseDurationMillis;

    /**
     * The time between two subsequent attempts to check and acquire the leadership.
     * It is randomized using the jitter factor.
     */
    private long retryPeriodMillis;

    public KubernetesLockConfiguration(JsonObject config) {
        this.configMapName = config.getString("leader-configmap", DEFAULT_LEADER_ELECTION_CONFIGMAP);
        this.jitterFactor = config.getDouble("jitter-factor", DEFAULT_JITTER_FACTOR);
        this.retryPeriodMillis = config.getLong("retry-period-millis", DEFAULT_RETRY_PERIOD_MILLIS);
        this.leaseDurationMillis = config.getLong("lease-duration-millis", DEFAULT_LEASE_DURATION_MILLIS);
        this.groupName = config.getString("group-name", DEFAULT_GROUP_NAME);
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public String getKubernetesResourcesNamespaceOrDefault(KubernetesClient kubernetesClient) {
        return kubernetesClient.getNamespace();
    }

    public String getConfigMapName() {
        return configMapName;
    }

    public Map<String, String> getClusterLabels() {
        return clusterLabels;
    }

    public String getGroupName() {
        return groupName;
    }

    public double getJitterFactor() {
        return jitterFactor;
    }

    public long getLeaseDurationMillis() {
        return leaseDurationMillis;
    }

    public long getRetryPeriodMillis() {
        return retryPeriodMillis;
    }

}
