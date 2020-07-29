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
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PodUnknownGcController extends BaseGcController<V1Pod> {
    public static final String POD_UNKNOWN_GC_CONTROLLER = "podUnknownGcController";
    public static final String POD_UNKNOWN_GC_CONTROLLER_DESCRIPTION = "GC pods that are unknown to Titus.";

    private static final Logger logger = LoggerFactory.getLogger(PodUnknownGcController.class);
    private final KubeApiFacade kubeApiFacade;
    private final Clock clock;
    private final KubeControllerConfiguration kubeControllerConfiguration;
    private final V3JobOperations v3JobOperations;

    @Inject
    public PodUnknownGcController(
            TitusRuntime titusRuntime,
            LocalScheduler scheduler,
            @Named(POD_UNKNOWN_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
            @Named(POD_UNKNOWN_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
            KubeApiFacade kubeApiFacade,
            KubeControllerConfiguration kubeControllerConfiguration,
            V3JobOperations v3JobOperations
    ) {
        super(
                POD_UNKNOWN_GC_CONTROLLER,
                POD_UNKNOWN_GC_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );
        this.kubeApiFacade = kubeApiFacade;
        this.kubeControllerConfiguration = kubeControllerConfiguration;
        this.clock = titusRuntime.getClock();
        this.v3JobOperations = v3JobOperations;
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getPodInformer().hasSynced();
    }

    @Override
    public List<V1Pod> getItemsToGc() {
        Map<String, Task> currentTasks = v3JobOperations.getTasks().stream()
                .collect(Collectors.toMap(Task::getId, Function.identity()));
        return kubeApiFacade.getPodInformer().getIndexer().list().stream()
                .filter(p -> isPodUnknownToJobManagement(p, currentTasks))
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1Pod item) {
        return GcControllerUtil.deletePod(kubeApiFacade, logger, item);
    }

    @VisibleForTesting
    boolean isPodUnknownToJobManagement(V1Pod pod, Map<String, Task> currentTasks) {
        V1ObjectMeta metadata = pod.getMetadata();
        V1PodStatus status = pod.getStatus();

        if (metadata == null || status == null) {
            // this pod is missing data so GC it
            return true;
        }

        if (KubeUtil.isPodPhaseTerminal(status.getPhase()) || currentTasks.containsKey(metadata.getName())) {
            return false;
        }

        DateTime creationTimestamp = metadata.getCreationTimestamp();
        return creationTimestamp != null &&
                clock.isPast(creationTimestamp.getMillis() + kubeControllerConfiguration.getPodUnknownGracePeriodMs());
    }
}