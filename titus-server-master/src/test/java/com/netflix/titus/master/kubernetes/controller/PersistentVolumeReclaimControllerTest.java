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

import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeStatus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class PersistentVolumeReclaimControllerTest {

    private static final String PERSISTENT_VOLUME_NAME = "vol-1";

    private final TitusRuntime titusRuntime = TitusRuntimes.test();
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final StdKubeApiFacade kubeApiFacade = mock(StdKubeApiFacade.class);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);

    private final PersistentVolumeReclaimController pvcReclaimController = new PersistentVolumeReclaimController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            kubeApiFacade
    );

    /**
     * Tests that a bound VPC is not selected for reclamation.
     */
    @Test
    public void testBoundPvcIsNotReclaimed() {
        V1PersistentVolume v1PersistentVolume = new V1PersistentVolume()
                .metadata(new V1ObjectMeta()
                        .name(PERSISTENT_VOLUME_NAME))
                .status(new V1PersistentVolumeStatus()
                        .phase("Bound"));
        assertThat(pvcReclaimController.isPvReleased(v1PersistentVolume)).isFalse();
    }

    /**
     * Tests that a released PVC is reclaimed.
     */
    @Test
    public void testReleasedPvcIsReclaimed() {
        V1PersistentVolume v1PersistentVolume = new V1PersistentVolume()
                .metadata(new V1ObjectMeta()
                        .name(PERSISTENT_VOLUME_NAME))
                .status(new V1PersistentVolumeStatus()
                        .phase("Released"));
        assertThat(pvcReclaimController.isPvReleased(v1PersistentVolume)).isTrue();
    }
}
