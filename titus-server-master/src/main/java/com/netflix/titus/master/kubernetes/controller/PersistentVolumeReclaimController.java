/*
 * Copyright 2021 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.master.kubernetes.controller;

import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeBuilder;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpecBuilder;
import io.kubernetes.client.openapi.models.V1PersistentVolumeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.mesos.kubeapiserver.KubeObjectFormatter.formatPvEssentials;

/**
 * Reclaims persistent volumes that are released from persistent volume claims that no longer exist.
 */
@Singleton
public class PersistentVolumeReclaimController extends BaseGcController<V1PersistentVolume> {

    private static final Logger logger = LoggerFactory.getLogger(PersistentVolumeReclaimController.class);

    public static final String PERSISTENT_VOLUME_RECLAIM_CONTROLLER = "persistentVolumeReclaimController";
    public static final String PERSISTENT_VOLUME_RECLAIM_CONTROLLER_DESCRIPTION = "GC persistent volumes that are no longer associated with active jobs.";

    private final KubeApiFacade kubeApiFacade;

    @Inject
    public PersistentVolumeReclaimController(TitusRuntime titusRuntime,
                                             LocalScheduler scheduler,
                                             @Named(PERSISTENT_VOLUME_RECLAIM_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
                                             @Named(PERSISTENT_VOLUME_RECLAIM_CONTROLLER) ControllerConfiguration controllerConfiguration,
                                             KubeApiFacade kubeApiFacade) {
        super(
                PERSISTENT_VOLUME_RECLAIM_CONTROLLER,
                PERSISTENT_VOLUME_RECLAIM_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );

        this.kubeApiFacade = kubeApiFacade;
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getPersistentVolumeInformer().hasSynced();
    }

    @Override
    public List<V1PersistentVolume> getItemsToGc() {
        return kubeApiFacade.getPersistentVolumeInformer().getIndexer().list().stream()
                // Only consider PVs that have been Released (i.e., the PVC in its claimRef has been deleted).
                .filter(this::isPvReleased)
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1PersistentVolume v1PersistentVolume) {
        logger.info("Reclaiming pv {}", formatPvEssentials(v1PersistentVolume));

        V1PersistentVolumeSpec spec = v1PersistentVolume.getSpec() == null ? new V1PersistentVolumeSpec() : v1PersistentVolume.getSpec();
        // Reclaim the pv by removing the claimRef.
        V1PersistentVolume updatedPv = new V1PersistentVolumeBuilder(v1PersistentVolume)
                .withSpec(new V1PersistentVolumeSpecBuilder(spec).build().claimRef(null))
                .build();
        try {
            kubeApiFacade.getCoreV1Api().replacePersistentVolume(
                    KubeUtil.getMetadataName(updatedPv.getMetadata()),
                    updatedPv,
                    null,
                    null,
                    null
            );
            logger.info("Successfully reclaimed pv {}", formatPvEssentials(updatedPv));
        } catch (Exception e) {
            logger.error("Failed to reclaim pv {} with error: ", formatPvEssentials(v1PersistentVolume), e);
            return false;
        }

        return true;
    }

    @VisibleForTesting
    boolean isPvReleased(V1PersistentVolume v1PersistentVolume) {
        V1PersistentVolumeStatus status = v1PersistentVolume.getStatus() == null
                ? new V1PersistentVolumeStatus()
                : v1PersistentVolume.getStatus();
        return status.getPhase() == null || status.getPhase().equalsIgnoreCase("Released");
    }
}
