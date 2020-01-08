package com.redhat.emergency.response.incident.priority.ha;

import com.redhat.emergency.response.incident.priority.ha.infra.election.KubernetesLockConfiguration;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoreKube {

    private static final Logger logger = LoggerFactory.getLogger(CoreKube.class);
    private KubernetesClient kubernetesClient ;
    private KubernetesLockConfiguration configuration;

    public CoreKube(JsonObject config) {
        kubernetesClient = new DefaultKubernetesClient();
        configuration = createKubeConfiguration(config);
    }

    private KubernetesLockConfiguration createKubeConfiguration(JsonObject config) {
        String podName = System.getenv("POD_NAME");
        if (podName == null) {
            podName = System.getenv("HOSTNAME");
        }
        if (logger.isInfoEnabled()) {
            logger.info("PodName: {}", podName);
        }
        KubernetesLockConfiguration configuration = new KubernetesLockConfiguration(config);
        configuration.setPodName(podName);
        return configuration;
    }

    public KubernetesClient getKubernetesClient() {
        return kubernetesClient;
    }

    public KubernetesLockConfiguration getConfiguration() {
        return configuration;
    }
}
