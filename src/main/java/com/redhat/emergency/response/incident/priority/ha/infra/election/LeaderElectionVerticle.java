package com.redhat.emergency.response.incident.priority.ha.infra.election;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.reactivex.Completable;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import kafka.utils.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UseOfObsoleteDateTimeApi")
public class LeaderElectionVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(LeaderElectionVerticle.class);

    private KubernetesClient kubernetesClient;

    private KubernetesLockConfiguration lockConfiguration;

    private State currentState = State.REPLICA;

    private ScheduledExecutorService serializedExecutor;

    private ConfigMap latestConfigMap;

    private Set<String> latestMembers;

    private LeaderInfo latestLeaderInfo;

    private List<LeadershipCallback> callbacks;

    public LeaderElectionVerticle(KubernetesClient kubernetesClient, KubernetesLockConfiguration lockConfiguration, State initialState) {
        this.kubernetesClient = kubernetesClient;
        this.lockConfiguration = lockConfiguration;
        this.callbacks = new ArrayList<>();
        if (initialState != null) {
            this.currentState = initialState;
        }
    }

    @Override
    public Completable rxStart() {
        log.debug("Starting LeaderElectionVerticle");
        vertx.eventBus().consumer("leader-election-start", this::startLeaderElection);
        return Completable.complete();
    }

    @Override
    public Completable rxStop() {
        log.debug("{} Stopping leadership election...", logPrefix());
        if (serializedExecutor != null) {
            serializedExecutor.shutdownNow();
        }
        serializedExecutor = null;
        return Completable.complete();
    }

    private void startLeaderElection(Message<JsonObject> tMessage) {
        if (serializedExecutor == null) {
            log.debug("{} Starting leadership election...", logPrefix());
            serializedExecutor = Executors.newSingleThreadScheduledExecutor();
            serializedExecutor.execute(this::refreshStatus);
        }
    }

    private void refreshStatus() {
        switch (currentState) {
            case REPLICA:
                refreshStatusNotLeader();
                break;
            case BECOMING_LEADER:
                refreshStatusBecomingLeader();
                break;
            case LEADER:
                refreshStatusLeader();
                break;
            default:
                throw new RuntimeException("Unsupported state " + currentState);
        }

        //TODO: notify other verticles
    }

    private void rescheduleAfterDelay() {
        this.serializedExecutor.schedule(this::refreshStatus,
                jitter(this.lockConfiguration.getRetryPeriodMillis(), this.lockConfiguration.getJitterFactor()),
                TimeUnit.MILLISECONDS);
    }

    private void refreshStatusNotLeader() {
        log.debug("{} Pod is not leader, pulling new data from the cluster", logPrefix());
        boolean pulled = lookupNewLeaderInfo();
        if (!pulled) {
            rescheduleAfterDelay();
            return;
        }

        if (this.latestLeaderInfo.hasEmptyLeader()) {
            // There is no previous leader
            if (log.isInfoEnabled()) {
                log.info("{} The cluster has no leaders. Trying to acquire the leadership...", logPrefix());
            }
            boolean acquired = tryAcquireLeadership();
            if (acquired) {
                if (log.isInfoEnabled()) {
                    log.info("{} Leadership acquired by current pod with immediate effect", logPrefix());
                }
                this.currentState = State.LEADER;
                this.refreshStatus();
                return;
            } else {
                if (log.isInfoEnabled()) {
                    log.info("{} Unable to acquire the leadership, it may have been acquired by another pod", logPrefix());
                }
            }
        } else if (!GlobalState.isCanBecomeLeader()) {
            // Node is waiting for an initial state to use as starting point
            log.info("{} Pod is not initialized yet (waiting snapshot) so cannot try to become leader", logPrefix());
            rescheduleAfterDelay();
            return;
        } else if (!this.latestLeaderInfo.hasValidLeader()) {
            // There's a previous leader and it's invalid
            log.info("{} Leadership has been lost by old owner. Trying to acquire the leadership...", logPrefix());
            boolean acquired = tryAcquireLeadership();
            if (acquired) {
                if (log.isInfoEnabled()) {
                    log.info("{} Leadership acquired by current pod", logPrefix());
                }
                this.currentState = State.BECOMING_LEADER;
                this.refreshStatus();
                return;
            } else {
                if (log.isInfoEnabled()) {
                    log.info("{} Unable to acquire the leadership, it may have been acquired by another pod", logPrefix());
                }
            }
        } else if (this.latestLeaderInfo.isValidLeader(this.lockConfiguration.getPodName())) {
            // We are leaders for some reason (e.g. pod restart on failure)
            if (log.isInfoEnabled()) {
                log.info("{} Leadership is already owned by current pod", logPrefix());
            }
            this.currentState = State.BECOMING_LEADER;
            this.refreshStatus();
            return;
        }
        rescheduleAfterDelay();
    }

    /**
     * This pod has acquired the leadership but it should wait for the old leader
     * to tear down resources before starting the local services.
     */
    private void refreshStatusBecomingLeader() {
        // Wait always the same amount of time before becoming the leader
        // Even if the current pod is already leader, we should let a possible old version of the pod to shut down
        long delay = this.lockConfiguration.getLeaseDurationMillis();
        if (log.isInfoEnabled()) {
            log.info("{} Current pod owns the leadership, but it will be effective in {} seconds...",
                    logPrefix(),
                    new BigDecimal(delay).divide(BigDecimal.valueOf(1000), 2, RoundingMode.HALF_UP));
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            log.warn("Thread interrupted",
                    e);
        }
        if (log.isInfoEnabled()) {
            log.info("{} Current pod is becoming the new LEADER now...",
                    logPrefix());
        }
        this.currentState = State.LEADER;
        this.serializedExecutor.execute(this::refreshStatus);
    }

    private void refreshStatusLeader() {
        log.debug("{} Pod should be the leader, pulling new data from the cluster", logPrefix());
        boolean pulled = lookupNewLeaderInfo();
        if (!pulled) {
            rescheduleAfterDelay();
            return;
        }

        if (this.latestLeaderInfo.isValidLeader(this.lockConfiguration.getPodName())) {
            if (log.isDebugEnabled()) {
                log.debug("{} Current Pod is still the leader", logPrefix());
            }
            rescheduleAfterDelay();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("{} Current Pod has lost the leadership", logPrefix());
            }
            this.currentState = State.REPLICA;

            // restart from scratch to acquire leadership
            this.serializedExecutor.execute(this::refreshStatus);
        }
    }

    private boolean lookupNewLeaderInfo() {
        if (log.isDebugEnabled()) {
            log.debug("{} Looking up leadership information...", logPrefix());
        }

        ConfigMap configMap;
        try {
            configMap = pullConfigMap();
        } catch (Throwable e) {
            log.warn(logPrefix() + " Unable to retrieve the current ConfigMap " + this.lockConfiguration.getConfigMapName() + " from Kubernetes");
            log.debug(logPrefix() + " Exception thrown during ConfigMap lookup", e);
            return false;
        }

        Set<String> members;
        try {
            members = Objects.requireNonNull(pullClusterMembers(), "Retrieved a null set of members");
        } catch (Throwable e) {
            log.warn(logPrefix() + " Unable to retrieve the list of cluster members from Kubernetes");
            log.debug(logPrefix() + " Exception thrown during Pod list lookup", e);
            return false;
        }

        updateLatestLeaderInfo(configMap, members);
        return true;
    }

    private boolean tryAcquireLeadership() {
        if (log.isDebugEnabled()) {
            log.debug("{} Trying to acquire the leadership...", logPrefix());
        }

        ConfigMap configMap = this.latestConfigMap;
        Set<String> members = this.latestMembers;
        LeaderInfo latestLeaderInfo = this.latestLeaderInfo;

        if (latestLeaderInfo == null || members == null) {
            if (log.isWarnEnabled()) {
                log.warn(logPrefix() + " Unexpected condition. Latest leader info or list of members is empty.");
            }
            return false;
        } else if (!members.contains(this.lockConfiguration.getPodName())) {
            log.warn(logPrefix() + " The list of cluster members " + latestLeaderInfo.getMembers() + " does not contain the current Pod. Cannot acquire"
                    + " leadership.");
            return false;
        }

        // Info we would set set in the configmap to become leaders
        LeaderInfo newLeaderInfo = new LeaderInfo(this.lockConfiguration.getGroupName(),
                this.lockConfiguration.getPodName(), new Date(), members);

        if (configMap == null) {
            // No ConfigMap created so far
            if (log.isDebugEnabled()) {
                log.debug("{} Lock configmap is not present in the Kubernetes namespace. A new ConfigMap will be created",
                        logPrefix());
            }
            ConfigMap newConfigMap = ConfigMapLockUtils.createNewConfigMap(this.lockConfiguration.getConfigMapName(),
                    newLeaderInfo);

            try {
                kubernetesClient.configMaps()
                        .inNamespace(this.lockConfiguration.getKubernetesResourcesNamespaceOrDefault(kubernetesClient))
                        .create(newConfigMap);
                if (log.isDebugEnabled()) {
                    log.debug("{} ConfigMap {} successfully created", logPrefix(), this.lockConfiguration.getConfigMapName());
                }
                updateLatestLeaderInfo(newConfigMap, members);
                return true;
            } catch (Exception ex) {
                // Suppress exception
                log.warn(logPrefix() + " Unable to create the ConfigMap, it may have been created by other cluster members concurrently. "
                        + "If the problem persists, check if the service account has the right permissions to create it");
                log.debug(logPrefix() + " Exception while trying to create the ConfigMap", ex);
                return false;
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("{} Lock configmap already present in the Kubernetes namespace. Checking...", logPrefix());
            }
            LeaderInfo leaderInfo = ConfigMapLockUtils.getLeaderInfo(configMap, members, this.lockConfiguration.getGroupName());

            boolean canAcquire = !leaderInfo.hasValidLeader();
            if (canAcquire) {
                // Try to be the new leader
                try {
                    ConfigMap updatedConfigMap = ConfigMapLockUtils.getConfigMapWithNewLeader(configMap, newLeaderInfo);
                    kubernetesClient.configMaps()
                            .inNamespace(this.lockConfiguration.getKubernetesResourcesNamespaceOrDefault(kubernetesClient))
                            .withName(this.lockConfiguration.getConfigMapName())
                            .lockResourceVersion(configMap.getMetadata().getResourceVersion())
                            .replace(updatedConfigMap);
                    if (log.isDebugEnabled()) {
                        log.debug("{} ConfigMap {} successfully updated",
                                logPrefix(),
                                this.lockConfiguration.getConfigMapName());
                    }
                    updateLatestLeaderInfo(updatedConfigMap, members);
                    return true;
                } catch (Exception ex) {
                    log.warn(logPrefix() + " Unable to update the lock ConfigMap to set leadership information");
                    log.debug(logPrefix() + " Error received during configmap lock replace", ex);
                    return false;
                }
            } else {
                // Another pod is the leader and it's still active
                if (log.isDebugEnabled()) {
                    log.debug("{} Another Pod ({}) is the current leader and it is still active", logPrefix(), this.latestLeaderInfo.getLeader());
                }
                return false;
            }
        }
    }

    private ConfigMap pullConfigMap() {
        return kubernetesClient.configMaps()
                .inNamespace(this.lockConfiguration.getKubernetesResourcesNamespaceOrDefault(kubernetesClient))
                .withName(this.lockConfiguration.getConfigMapName())
                .get();
    }

    private Set<String> pullClusterMembers() {
        List<Pod> pods = kubernetesClient.pods()
                .inNamespace(this.lockConfiguration.getKubernetesResourcesNamespaceOrDefault(kubernetesClient))
                .withLabels(this.lockConfiguration.getClusterLabels())
                .list().getItems();

        return pods.stream().map(pod -> pod.getMetadata().getName()).collect(Collectors.toSet());
    }

    private void updateLatestLeaderInfo(ConfigMap configMap,
                                        Set<String> members) {
        log.debug("{} Updating internal status about the current leader", logPrefix());
        this.latestConfigMap = configMap;
        this.latestMembers = members;
        this.latestLeaderInfo = ConfigMapLockUtils.getLeaderInfo(configMap, members, this.lockConfiguration.getGroupName());
        log.debug("{} Current leader info: {}", logPrefix(), this.latestLeaderInfo);
    }

    private long jitter(long num, double factor) {
        return (long) (num * (1 + Math.random() * (factor - 1)));
    }

    private String logPrefix() {
        return "Pod[" + this.lockConfiguration.getPodName() + "]";
    }
}
