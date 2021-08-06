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
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.KubeModelConverters;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.model.job.TaskState.isTerminalState;
import static com.netflix.titus.master.mesos.kubeapiserver.KubeObjectFormatter.formatPvcEssentials;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.DEFAULT_NAMESPACE;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.NOT_FOUND;

/**
 * Garbage collects persistent volume claims that are not associated with active/non-terminal tasks.
 */
@Singleton
public class PersistentVolumeClaimGcController extends BaseGcController<V1PersistentVolumeClaim> {

    private static final Logger logger = LoggerFactory.getLogger(PersistentVolumeClaimGcController.class);

    public static final String PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER = "persistentVolumeClaimGcController";
    public static final String PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER_DESCRIPTION = "GC persistent volume claims that are no longer associated with active tasks.";

    private final KubeApiFacade kubeApiFacade;
    private final V3JobOperations v3JobOperations;

    @Inject
    public PersistentVolumeClaimGcController(TitusRuntime titusRuntime,
                                             LocalScheduler scheduler,
                                             @Named(PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
                                             @Named(PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
                                             KubeApiFacade kubeApiFacade,
                                             V3JobOperations v3JobOperations) {
        super(
                PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER,
                PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );

        this.kubeApiFacade = kubeApiFacade;
        this.v3JobOperations = v3JobOperations;
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getPersistentVolumeInformer().hasSynced();
    }

    @Override
    public List<V1PersistentVolumeClaim> getItemsToGc() {
        return kubeApiFacade.getPersistentVolumeClaimInformer().getIndexer().list();
    }

    @Override
    public boolean gcItem(V1PersistentVolumeClaim pvc) {
        // Extract the task ID embedded in the PVC's name
        String taskId = KubeModelConverters.getTaskIdFromPvc(pvc);

        Optional<Pair<Job<?>, Task>> optionalJobTaskPair = v3JobOperations.findTaskById(taskId);
        if (!optionalJobTaskPair.isPresent()) {
            // If we could not find a task for the PVC, GC it.
            logger.info("Did not find task {} for pvc {}", taskId, formatPvcEssentials(pvc));
            return gcPersistentVolumeClaim(pvc);
        }
        // If the task is terminal, GC it.
        return isTerminalState(optionalJobTaskPair.get().getRight().getStatus().getState()) && gcPersistentVolumeClaim(pvc);
    }

    private boolean gcPersistentVolumeClaim(V1PersistentVolumeClaim pvc) {
        String volumeClaimName = KubeUtil.getMetadataName(pvc.getMetadata());
        try {
            // If the PVC is deleted while still in use by a pod (though that is not expected), the PVC
            // will not be removed until no pod is using it.
            // https://kubernetes.io/docs/concepts/storage/persistent-volumes/#storage-object-in-use-protection
            kubeApiFacade.deleteNamespacedPersistentVolumeClaim(DEFAULT_NAMESPACE, volumeClaimName);
            logger.info("Successfully deleted persistent volume claim {}", formatPvcEssentials(pvc));
            return true;
        } catch (ApiException e) {
            if (!e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                // If we did not find the PVC return true as it is removed
                logger.info("Delete for persistent volume claim {} not found", formatPvcEssentials(pvc));
                return true;
            }
            logger.error("Failed to delete persistent volume claim: {} with error: ", formatPvcEssentials(pvc), e);
        } catch (Exception e) {
            logger.error("Failed to delete persistent volume claim: {} with error: ", formatPvcEssentials(pvc), e);
        }
        return false;
    }
}
