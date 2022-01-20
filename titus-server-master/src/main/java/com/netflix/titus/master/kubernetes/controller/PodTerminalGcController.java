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
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.kubernetes.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PodTerminalGcController extends BaseGcController<V1Pod> {
    public static final String POD_TERMINAL_GC_CONTROLLER = "podTerminalGcController";
    public static final String POD_TERMINAL_GC_CONTROLLER_DESCRIPTION = "GC pods that are terminal to Titus.";

    private static final Logger logger = LoggerFactory.getLogger(PodTerminalGcController.class);
    private final StdKubeApiFacade kubeApiFacade;
    private final Clock clock;
    private final KubeControllerConfiguration kubeControllerConfiguration;
    private final V3JobOperations v3JobOperations;

    @Inject
    public PodTerminalGcController(
            TitusRuntime titusRuntime,
            @Named(GC_CONTROLLER) LocalScheduler scheduler,
            @Named(POD_TERMINAL_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
            @Named(POD_TERMINAL_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
            StdKubeApiFacade kubeApiFacade,
            KubeControllerConfiguration kubeControllerConfiguration,
            V3JobOperations v3JobOperations
    ) {
        super(
                POD_TERMINAL_GC_CONTROLLER,
                POD_TERMINAL_GC_CONTROLLER_DESCRIPTION,
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
                .filter(p -> isPodTerminal(p, currentTasks))
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1Pod item) {
        return GcControllerUtil.deletePod(kubeApiFacade, logger, item);
    }

    @VisibleForTesting
    boolean isPodTerminal(V1Pod pod, Map<String, Task> currentTasks) {
        String podName = KubeUtil.getMetadataName(pod.getMetadata());
        Task task = currentTasks.get(podName);
        if (task != null) {
            if (TaskState.isTerminalState(task.getStatus().getState())) {
                return clock.isPast(task.getStatus().getTimestamp() + kubeControllerConfiguration.getPodTerminalGracePeriodMs());
            }
            return false;
        }

        V1PodStatus status = pod.getStatus();
        if (status != null && KubeUtil.isPodPhaseTerminal(status.getPhase())) {
            return KubeUtil.findFinishedTimestamp(pod)
                    .map(timestamp -> clock.isPast(timestamp + kubeControllerConfiguration.getPodTerminalGracePeriodMs()))
                    .orElse(true);
        }
        return false;
    }
}