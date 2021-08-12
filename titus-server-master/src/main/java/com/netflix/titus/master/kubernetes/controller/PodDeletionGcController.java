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

import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.PENDING;

@Singleton
public class PodDeletionGcController extends BaseGcController<V1Pod> {
    public static final String POD_DELETION_GC_CONTROLLER = "podDeletionGcController";
    public static final String POD_DELETION_GC_CONTROLLER_DESCRIPTION = "GC pods with a deletion timestamp that needs to be cleaned up.";

    private static final Logger logger = LoggerFactory.getLogger(PodDeletionGcController.class);
    private final KubeApiFacade kubeApiFacade;
    private final Clock clock;
    private final KubeControllerConfiguration kubeControllerConfiguration;

    @Inject
    public PodDeletionGcController(
            TitusRuntime titusRuntime,
            LocalScheduler scheduler,
            @Named(POD_DELETION_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
            @Named(POD_DELETION_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
            KubeApiFacade kubeApiFacade,
            KubeControllerConfiguration kubeControllerConfiguration
    ) {
        super(
                POD_DELETION_GC_CONTROLLER,
                POD_DELETION_GC_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );
        this.kubeApiFacade = kubeApiFacade;
        this.kubeControllerConfiguration = kubeControllerConfiguration;
        this.clock = titusRuntime.getClock();
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getNodeInformer().hasSynced() && kubeApiFacade.getPodInformer().hasSynced();
    }

    @Override
    public List<V1Pod> getItemsToGc() {
        return kubeApiFacade.getPodInformer().getIndexer().list()
                .stream()
                .filter(p -> isPodInPendingPhaseWithDeletionTimestamp(p) || isPodPastDeletionTimestamp(p))
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1Pod item) {
        return GcControllerUtil.deletePod(kubeApiFacade, logger, item);
    }

    @VisibleForTesting
    boolean isPodInPendingPhaseWithDeletionTimestamp(V1Pod pod) {
        if (pod == null || pod.getMetadata() == null || pod.getStatus() == null) {
            return false;
        }
        OffsetDateTime deletionTimestamp = pod.getMetadata().getDeletionTimestamp();
        return deletionTimestamp != null && PENDING.equalsIgnoreCase(pod.getStatus().getPhase());
    }

    @VisibleForTesting
    boolean isPodPastDeletionTimestamp(V1Pod pod) {
        V1PodSpec spec = pod.getSpec();
        V1ObjectMeta metadata = pod.getMetadata();
        if (spec == null || metadata == null) {
            return false;
        }

        Long terminationGracePeriodSeconds = spec.getTerminationGracePeriodSeconds();
        OffsetDateTime deletionTimestamp = metadata.getDeletionTimestamp();
        if (terminationGracePeriodSeconds == null || deletionTimestamp == null) {
            return false;
        }

        long terminationGracePeriodMs = terminationGracePeriodSeconds * 1000L;
        return clock.isPast(deletionTimestamp.toInstant().toEpochMilli() + terminationGracePeriodMs
                + kubeControllerConfiguration.getPodsPastTerminationGracePeriodMs());
    }
}