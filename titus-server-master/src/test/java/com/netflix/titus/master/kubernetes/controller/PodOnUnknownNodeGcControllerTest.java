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

import java.util.Collections;

import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.time.internal.DefaultTestClock;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

public class PodOnUnknownNodeGcControllerTest {
    private static final String NODE_NAME = "node-name";
    private static final String POD_NAME = "pod-name";
    private static final TestClock clock = new DefaultTestClock();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final KubeApiFacade kubeApiFacade = mock(KubeApiFacade.class);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);

    private final PodOnUnknownNodeGcController podGcController = new PodOnUnknownNodeGcController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            kubeApiFacade
    );

    /**
     * The pod has a node name for a node that is not known and should return true.
     */
    @Test
    void podIsOnUnknownNode() {
        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta().name(POD_NAME))
                .spec(new V1PodSpec().nodeName(NODE_NAME))
                .status(null);

        Assertions.assertThat(podGcController.isPodOnUnknownNode(pod, Collections.emptySet())).isTrue();
    }

    /**
     * The pod has a node name for a node that is known and should return false.
     */
    @Test
    void podIsOnKnownNode() {
        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta().name(POD_NAME))
                .spec(new V1PodSpec().nodeName(NODE_NAME))
                .status(null);

        Assertions.assertThat(podGcController.isPodOnUnknownNode(pod, Collections.singleton(NODE_NAME))).isFalse();
    }
}