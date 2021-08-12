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

import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.time.internal.DefaultTestClock;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.PENDING;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.RUNNING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodDeletionGcControllerTest {
    private static final String NODE_NAME = "node-name";
    private static final String POD_NAME = "pod-name";
    private static final TestClock clock = new DefaultTestClock();
    private static final long POD_DELETION_TIMESTAMP_GRACE_PERIOD = 1000L;
    private static final long POD_TERMINATION_GRACE_PERIOD_SEC = 30L;
    private static final long POD_TERMINATION_GRACE_PERIOD_MS = POD_TERMINATION_GRACE_PERIOD_SEC * 1000;

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final KubeApiFacade kubeApiFacade = mock(KubeApiFacade.class);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);
    private final KubeControllerConfiguration kubeControllerConfiguration = mock(KubeControllerConfiguration.class);

    private final PodDeletionGcController podDeletionGcController = new PodDeletionGcController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            kubeApiFacade,
            kubeControllerConfiguration
    );


    @BeforeEach
    void setUp() {
        when(kubeControllerConfiguration.getPodsPastTerminationGracePeriodMs()).thenReturn(POD_DELETION_TIMESTAMP_GRACE_PERIOD);
    }

    /**
     * The pod has a deletion timestamp that is past the termination and deletion grace periods and should return true.
     */
    @Test
    void podIsPastDeletionTimestamp() {
        V1Pod pod = new V1Pod()
                .metadata(
                        new V1ObjectMeta()
                                .name(POD_NAME)
                                .deletionTimestamp(DateTimeExt.fromMillis(clock.wallTime()))
                )
                .spec(
                        new V1PodSpec()
                                .nodeName(NODE_NAME)
                                .terminationGracePeriodSeconds(POD_TERMINATION_GRACE_PERIOD_SEC))
                .status(null);

        clock.advanceTime(Duration.ofMillis(POD_TERMINATION_GRACE_PERIOD_MS + POD_DELETION_TIMESTAMP_GRACE_PERIOD + 1));

        Assertions.assertThat(podDeletionGcController.isPodPastDeletionTimestamp(pod)).isTrue();
    }

    /**
     * The pod has a deletion timestamp that is not past the termination and deletion grace periods and should return false.
     */
    @Test
    void podIsNotPastDeletionTimestamp() {
        V1Pod pod = new V1Pod()
                .metadata(
                        new V1ObjectMeta()
                                .name(POD_NAME)
                                .deletionTimestamp(DateTimeExt.fromMillis(clock.wallTime()))
                )
                .spec(
                        new V1PodSpec()
                                .nodeName(NODE_NAME)
                                .terminationGracePeriodSeconds(POD_TERMINATION_GRACE_PERIOD_SEC))
                .status(null);

        Assertions.assertThat(podDeletionGcController.isPodPastDeletionTimestamp(pod)).isFalse();
    }

    /**
     * The pod has a deletion timestamp and is in the Pending phase so should return true.
     */
    @Test
    void podIsPendingPhaseWithDeletionTimestamp() {
        V1Pod pod = new V1Pod()
                .metadata(
                        new V1ObjectMeta()
                                .name(POD_NAME)
                                .deletionTimestamp(DateTimeExt.fromMillis(clock.wallTime()))
                )
                .spec(new V1PodSpec())
                .status(new V1PodStatus().phase(PENDING));

        Assertions.assertThat(podDeletionGcController.isPodInPendingPhaseWithDeletionTimestamp(pod)).isTrue();
    }

    /**
     * The pod has a deletion timestamp and is in the Running phase so should return false.
     */
    @Test
    void podIsInRunningPhaseWithDeletionTimestamp() {
        V1Pod pod = new V1Pod()
                .metadata(
                        new V1ObjectMeta()
                                .name(POD_NAME)
                                .deletionTimestamp(DateTimeExt.fromMillis(clock.wallTime()))
                )
                .spec(new V1PodSpec())
                .status(new V1PodStatus().phase(RUNNING));

        Assertions.assertThat(podDeletionGcController.isPodInPendingPhaseWithDeletionTimestamp(pod)).isFalse();
    }

    /**
     * The pod does not have a deletion timestamp and is in the Pending phase so should return false.
     */
    @Test
    void podDoesNotHaveDeletionTimestamp() {
        V1Pod pod = new V1Pod()
                .metadata(
                        new V1ObjectMeta()
                                .name(POD_NAME)
                )
                .spec(new V1PodSpec())
                .status(new V1PodStatus().phase(PENDING));

        Assertions.assertThat(podDeletionGcController.isPodInPendingPhaseWithDeletionTimestamp(pod)).isFalse();
    }
}