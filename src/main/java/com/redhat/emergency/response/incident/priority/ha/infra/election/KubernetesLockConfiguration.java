package com.redhat.emergency.response.incident.priority.ha.infra.election;

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesLockConfiguration {

    public static final double DEFAULT_JITTER_FACTOR = 1.2;
    public static final long DEFAULT_LEASE_DURATION_MILLIS = 30000;
    public static final long DEFAULT_RENEW_DEADLINE_MILLIS = 20000;
    public static final long DEFAULT_RETRY_PERIOD_MILLIS = 5000;

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
    private double jitterFactor = DEFAULT_JITTER_FACTOR;

    /**
     * The default duration of the lease for the current leader.
     */
    private long leaseDurationMillis = DEFAULT_LEASE_DURATION_MILLIS;

    /**
     * The deadline after which the leader must stop its services because it may have lost the leadership.
     */
    private long renewDeadlineMillis = DEFAULT_RENEW_DEADLINE_MILLIS;

    /**
     * The time between two subsequent attempts to check and acquire the leadership.
     * It is randomized using the jitter factor.
     */
    private long retryPeriodMillis = DEFAULT_RETRY_PERIOD_MILLIS;

    public KubernetesLockConfiguration(String configMapName) {
        this.configMapName = configMapName;
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

    public void setClusterLabels(Map<String, String> clusterLabels) {
        this.clusterLabels = clusterLabels;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public double getJitterFactor() {
        return jitterFactor;
    }

    public void setJitterFactor(double jitterFactor) {
        this.jitterFactor = jitterFactor;
    }

    public long getLeaseDurationMillis() {
        return leaseDurationMillis;
    }

    public void setLeaseDurationMillis(long leaseDurationMillis) {
        this.leaseDurationMillis = leaseDurationMillis;
    }

    public long getRenewDeadlineMillis() {
        return renewDeadlineMillis;
    }

    public void setRenewDeadlineMillis(long renewDeadlineMillis) {
        this.renewDeadlineMillis = renewDeadlineMillis;
    }

    public long getRetryPeriodMillis() {
        return retryPeriodMillis;
    }

    public void setRetryPeriodMillis(long retryPeriodMillis) {
        this.retryPeriodMillis = retryPeriodMillis;
    }
}
