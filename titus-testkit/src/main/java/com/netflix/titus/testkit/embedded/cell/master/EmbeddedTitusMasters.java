/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.cell.master;

import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters;

/**
 * A collection of preconfigured {@link EmbeddedTitusMaster} instances with different configuration tuning targets.
 */
public final class EmbeddedTitusMasters {

    /**
     * Embedded TitusMaster with configuration tuned up for faster execution, to make test fast.
     */
    public static EmbeddedTitusMaster basicMasterWithKubeIntegration(EmbeddedKubeCluster embeddedKubeCluster) {
        return EmbeddedTitusMaster.aTitusMaster()
                .withEmbeddedKubeCluster(embeddedKubeCluster)
                .withProperty("titus.agent.cacheRefreshIntervalMs", "500")
                .withProperty("titus.agent.fullCacheRefreshIntervalMs", "500")
                .withProperty("titus.agent.synchronizeWithInstanceCacheIntervalMs", "500")
                .withProperty("titus.master.capacityManagement.availableCapacityUpdateIntervalMs", "10")

                .withProperty("titus.scheduler.exitUponFenzoSchedulingErrorEnabled", "false")
                // Disable launch guard
                .withProperty("titus.scheduler.globalTaskLaunchingConstraintEvaluatorEnabled", "false")
                .withProperty("titus.scheduler.maxLaunchingTasksPerMachine", "96")

                // speed up scheduling during tests
                .withProperty("titus.scheduler.schedulerIterationIntervalMs", "1")
                .withProperty("titus.scheduler.schedulerMaxIdleIntervalMs", "5")

                .withProperty("titus.scheduler.tierSlaUpdateIntervalMs", "10")
                .withProperty("titus.master.grpcServer.shutdownTimeoutMs", "0")
                .withProperty("titusMaster.jobManager.taskInLaunchedStateTimeoutMs", "30000")
                .withProperty("titusMaster.jobManager.batchTaskInStartInitiatedStateTimeoutMs", "30000")
                .withProperty("titusMaster.jobManager.serviceTaskInStartInitiatedStateTimeoutMs", "30000")
                .withProperty("titus.master.taskMigration.schedulerDelayInMillis", "100")
                .withProperty("titusMaster.jobManager.reconcilerIdleTimeoutMs", "100")
                .withProperty("titusMaster.jobManager.reconcilerActiveTimeoutMs", "10")
                .withProperty("titus.master.grpcServer.loadbalancer.enabled", "true")
                .withProperty("titus.master.loadBalancer.engineEnabled", "true")
                .withProperty("titusMaster.eviction.eventStreamQuotaUpdateIntervalMs", "100")
                .withProperty("titus.features.jobManager.disruptionBudget.whiteList", ".*")
                .withProperty("titus.feature.kubeSchedulerEnabled", "true")
                .withProperty("titusMaster.kubernetes.pod.gpuResourcePoolNames", EmbeddedKubeClusters.RESOURCE_POOL_GPU)
                .withProperty("titusMaster.directKube.ebsVolumePvEnabled", "true")
                .build();
    }
}
