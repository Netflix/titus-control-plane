/*
 * Copyright 2020 Netflix, Inc.
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.mesos.kubeapiserver.KubeObjectFormatter.formatPvEssentials;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.NOT_FOUND;

/**
 * Garbage collects persistent volumes that are not associated with active/non-terminal jobs.
 */
@Singleton
public class PersistentVolumeUnassociatedGcController extends BaseGcController<V1PersistentVolume> {
    private static final Logger logger = LoggerFactory.getLogger(PersistentVolumeUnassociatedGcController.class);

    public static final String PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER = "persistentVolumeUnassociatedGcController";
    public static final String PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER_DESCRIPTION = "GC persistent volumes that are no longer associated with active jobs.";

    private final KubeApiFacade kubeApiFacade;
    private final KubeControllerConfiguration kubeControllerConfiguration;
    private final V3JobOperations v3JobOperations;

    // Contains the persistent volumes marked for GC with the timestamp of when it was marked.
    private final Map<String, Long> markedPersistentVolumes = new ConcurrentHashMap<>();

    @Inject
    public PersistentVolumeUnassociatedGcController(TitusRuntime titusRuntime,
                                                    LocalScheduler scheduler,
                                                    @Named(PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
                                                    @Named(PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
                                                    KubeApiFacade kubeApiFacade,
                                                    KubeControllerConfiguration kubeControllerConfiguration,
                                                    V3JobOperations v3JobOperations) {
        super(
                PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER,
                PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );

        this.kubeApiFacade = kubeApiFacade;
        this.kubeControllerConfiguration = kubeControllerConfiguration;
        this.v3JobOperations = v3JobOperations;
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getPersistentVolumeInformer().hasSynced();
    }

    @Override
    public List<V1PersistentVolume> getItemsToGc() {
        Set<String> currentEbsVolumes = v3JobOperations.getJobs().stream()
                .flatMap(job -> job.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes().stream())
                .map(EbsVolume::getVolumeId)
                .collect(Collectors.toSet());
        return kubeApiFacade.getPersistentVolumeInformer().getIndexer().list().stream()
                // Only consider PVs that are available (i.e., not bound)
                .filter(pv -> (pv.getStatus() == null ? "" : pv.getStatus().getPhase()).equalsIgnoreCase("Available"))
                // Only consider PVs that are not associated with active jobs
                .filter(pv -> isPersistentVolumeUnassociated(pv, currentEbsVolumes))
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1PersistentVolume pv) {
        return gcPersistentVolume(pv);
    }

    private boolean gcPersistentVolume(V1PersistentVolume pv) {
        String volumeName = KubeUtil.getMetadataName(pv.getMetadata());
        try {
            // If the PV is deleted while still associated with a PVC (though that is not expected), the PV
            // will not be removed until it is no longer bound to a PVC.
            // https://kubernetes.io/docs/concepts/storage/persistent-volumes/#storage-object-in-use-protection
            kubeApiFacade.getCoreV1Api().deletePersistentVolume(
                    volumeName,
                    null,
                    null,
                    0,
                    null,
                    null,
                    null
            );
            logger.info("Successfully deleted persistent volume {}", formatPvEssentials(pv));
            return true;
        } catch (ApiException e) {
            if (!e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                // If we did not find the PV return true as it is removed
                logger.info("Delete for persistent volume {} not found", formatPvEssentials(pv));
                return true;
            }
            logger.error("Failed to delete persistent volume: {} with error: ", formatPvEssentials(pv), e);
        } catch (Exception e) {
            logger.error("Failed to delete persistent volume: {} with error: ", formatPvEssentials(pv), e);
        }
        return false;
    }

    /**
     * Returns true if the persistent volume has not been associated with an active job for enough time.
     */
    @VisibleForTesting
    boolean isPersistentVolumeUnassociated(V1PersistentVolume pv, Set<String> ebsVolumeIds) {
        V1ObjectMeta metadata = pv.getMetadata();
        V1PersistentVolumeStatus status = pv.getStatus();

        if (metadata == null || metadata.getName() == null || status == null) {
            // this persistent volume is missing data so GC it
            return true;
        }

        String volumeName = StringExt.nonNull(metadata.getName());
        if (ebsVolumeIds.contains(volumeName)) {
            // this persistent volume is associated with an active job, so reset/remove
            // any marking and don't GC it.
            markedPersistentVolumes.remove(volumeName);
            return false;
        }

        // TODO Maybe skip terminating as these should be handled already?
        // TODO Find/emit where volumes are stuck in terminating for too long like orphan pod

        Long marked = markedPersistentVolumes.get(volumeName);
        if (marked == null) {
            // this persistent volume is not associated with an active job and is not marked,
            // so mark it.
            markedPersistentVolumes.put(volumeName, titusRuntime.getClock().wallTime());
            return false;
        }

        // gc this persistent volume if it has been marked for long enough
        return titusRuntime.getClock().isPast(marked + kubeControllerConfiguration.getPersistentVolumeUnassociatedGracePeriodMs());
    }

}
