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

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.time.internal.DefaultTestClock;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeStatus;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistentVolumeUnassociatedGcControllerTest {

    private static final String PERSISTENT_VOLUME_NAME = "vol-1";
    private static final TestClock clock = new DefaultTestClock();
    private static final long PERSISTENT_VOLUME_GRACE_PERIOD_MS = 1000L;

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final StdKubeApiFacade kubeApiFacade = mock(StdKubeApiFacade.class);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);
    private final KubeControllerConfiguration kubeControllerConfiguration = mock(KubeControllerConfiguration.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
    private final CoreV1Api coreV1Api = mock(CoreV1Api.class);

    private final PersistentVolumeUnassociatedGcController pvGcController = new PersistentVolumeUnassociatedGcController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            kubeApiFacade,
            kubeControllerConfiguration,
            v3JobOperations
    );

    @Before
    public void setUp() {
        when(kubeControllerConfiguration.getPersistentVolumeUnassociatedGracePeriodMs()).thenReturn(PERSISTENT_VOLUME_GRACE_PERIOD_MS);
    }

    /**
     * Tests that a persistent volume that is not associated with a job for enough time is GC'd.
     */
    @Test
    public void testPvIsUnassociated() {
        V1PersistentVolume v1PersistentVolume = new V1PersistentVolume()
                .metadata(new V1ObjectMeta()
                        .name(PERSISTENT_VOLUME_NAME))
                .status(new V1PersistentVolumeStatus()
                        .phase("Available"));

        Set<String> currentEbsVolume = Collections.singleton("vol-2");

        // Initially checking this volume should mark it but not consider it unassociated
        assertThat(pvGcController.isPersistentVolumeUnassociated(v1PersistentVolume, currentEbsVolume)).isFalse();

        // Move time forward and expect the volume to be considered unassociated
        clock.advanceTime(Duration.ofMillis(PERSISTENT_VOLUME_GRACE_PERIOD_MS + 1));
        assertThat(pvGcController.isPersistentVolumeUnassociated(v1PersistentVolume, currentEbsVolume)).isTrue();
    }

    /**
     * Tests that a persistent volume that is associated with a current job is not GC'd.
     */
    @Test
    public void testPvIsAssociated() {
        V1PersistentVolume v1PersistentVolume = new V1PersistentVolume()
                .metadata(new V1ObjectMeta()
                        .name(PERSISTENT_VOLUME_NAME))
                .status(new V1PersistentVolumeStatus()
                        .phase("Available"));

        Set<String> currentEbsVolume = Collections.singleton(PERSISTENT_VOLUME_NAME);

        // Initially checking this volume should not mark it
        assertThat(pvGcController.isPersistentVolumeUnassociated(v1PersistentVolume, currentEbsVolume)).isFalse();

        // Move time forward and expect the volume to be GC'd
        clock.advanceTime(Duration.ofMillis(PERSISTENT_VOLUME_GRACE_PERIOD_MS + 1));
        assertThat(pvGcController.isPersistentVolumeUnassociated(v1PersistentVolume, currentEbsVolume)).isFalse();
    }
}
