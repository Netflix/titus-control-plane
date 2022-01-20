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

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimStatus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistentVolumeClaimGcControllerTest {

    private static final String PERSISTENT_VOLUME_NAME = "vol-1";
    private static final Job<BatchJobExt> BATCH_JOB = JobGenerator.oneBatchJob();
    private static final BatchJobTask BATCH_TASK = JobGenerator.batchTasks(BATCH_JOB).getValue();

    private final TitusRuntime titusRuntime = TitusRuntimes.test();
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final StdKubeApiFacade kubeApiFacade = mock(StdKubeApiFacade.class);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);

    private final PersistentVolumeClaimGcController pvcGcController = new PersistentVolumeClaimGcController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            kubeApiFacade,
            v3JobOperations
    );

    /**
     * Tests that a PVC that is Bound is not GC'd.
     */
    @Test
    public void testBoundPvc() {
        String taskId = BATCH_TASK.getId();
        V1PersistentVolumeClaim v1PersistentVolumeClaim = new V1PersistentVolumeClaim()
                .metadata(new V1ObjectMeta()
                        .name(PERSISTENT_VOLUME_NAME + "." + taskId))
                .status(new V1PersistentVolumeClaimStatus()
                        .phase("Bound"));

        when(v3JobOperations.findTaskById(taskId)).thenReturn(Optional.of(Pair.of(BATCH_JOB, BATCH_TASK)));

        assertThat(pvcGcController.gcItem(v1PersistentVolumeClaim)).isFalse();
    }

    /**
     * Tests that a PVC that is released is GC'd.
     */
    @Test
    public void testReleasedPvc() {
        String taskId = BATCH_TASK.getId();
        V1PersistentVolumeClaim v1PersistentVolumeClaim = new V1PersistentVolumeClaim()
                .metadata(new V1ObjectMeta()
                        .name(PERSISTENT_VOLUME_NAME + "." + taskId))
                .status(new V1PersistentVolumeClaimStatus()
                        .phase("Bound"));

        when(v3JobOperations.findTaskById(taskId)).thenReturn(Optional.empty());

        assertThat(pvcGcController.gcItem(v1PersistentVolumeClaim)).isTrue();
    }
}
