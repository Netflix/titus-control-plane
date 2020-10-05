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
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.time.internal.DefaultTestClock;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.RUNNING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodUnknownGcControllerTest {
    private static final String POD_NAME = "pod-name";
    private static final TestClock clock = new DefaultTestClock();
    private static final long POD_UNKNOWN_GRACE_PERIOD = 1000L;

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final KubeApiFacade kubeApiFacade = mock(KubeApiFacade.class);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);
    private final KubeControllerConfiguration kubeControllerConfiguration = mock(KubeControllerConfiguration.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);

    private final PodUnknownGcController podGcController = new PodUnknownGcController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            kubeApiFacade,
            kubeControllerConfiguration,
            v3JobOperations
    );

    /**
     * The pod is known to job management and should not be GC'ed
     */
    @Test
    void podIsKnownToJobManagement() {
        when(kubeControllerConfiguration.getPodUnknownGracePeriodMs()).thenReturn(POD_UNKNOWN_GRACE_PERIOD);

        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta().name(POD_NAME).creationTimestamp(new DateTime(clock.wallTime())))
                .status(new V1PodStatus().phase(RUNNING));

        clock.advanceTime(Duration.ofMillis(POD_UNKNOWN_GRACE_PERIOD + 1));

        Map<String, Task> currentTasks = Collections.singletonMap(POD_NAME, mock(Task.class));
        Assertions.assertThat(podGcController.isPodUnknownToJobManagement(pod, currentTasks)).isFalse();
    }

    /**
     * The pod is unknown to job management and should be GC'ed
     */
    @Test
    void podIsUnknownToJobManagement() {
        when(kubeControllerConfiguration.getPodUnknownGracePeriodMs()).thenReturn(POD_UNKNOWN_GRACE_PERIOD);

        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta().name(POD_NAME).creationTimestamp(new DateTime(clock.wallTime())))
                .status(new V1PodStatus().phase(RUNNING));

        clock.advanceTime(Duration.ofMillis(POD_UNKNOWN_GRACE_PERIOD + 1));

        Map<String, Task> currentTasks = Collections.singletonMap("", mock(Task.class));
        Assertions.assertThat(podGcController.isPodUnknownToJobManagement(pod, currentTasks)).isTrue();
    }
}