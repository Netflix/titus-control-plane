/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.aws.supervisor;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.google.common.util.concurrent.Futures;
import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.ext.aws.AwsConfiguration;
import org.junit.Before;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.ext.aws.supervisor.AsgLocalMasterReadinessResolver.REFRESH_SCHEDULER_DESCRIPTOR;
import static com.netflix.titus.ext.aws.supervisor.AsgLocalMasterReadinessResolver.TAG_MASTER_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsgLocalMasterReadinessResolverTest {

    private static final String ERROR_MARKER = "errorMarker";

    private static final RuntimeException SIMULATED_ERROR = new RuntimeException("simulated error");

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final AwsConfiguration configuration = mock(AwsConfiguration.class);

    private final AmazonAutoScalingAsync autoScalingClient = mock(AmazonAutoScalingAsync.class);
    private final AtomicReference<String> currentTagState = new AtomicReference<>();
    private final AtomicInteger invocationCounter = new AtomicInteger();

    private AsgLocalMasterReadinessResolver resolver;

    @Before
    public void setUp() {
        when(configuration.getTitusMasterAsgName()).thenReturn("MasterAsg");

        when(autoScalingClient.describeAutoScalingGroupsAsync(any(), any())).thenAnswer(invocation -> {
            DescribeAutoScalingGroupsRequest request = invocation.getArgument(0);
            AsyncHandler asyncHandler = invocation.getArgument(1);

            if (ERROR_MARKER.equals(currentTagState.get())) {
                asyncHandler.onError(SIMULATED_ERROR);
                invocationCounter.incrementAndGet();
                return Futures.immediateFailedFuture(SIMULATED_ERROR);
            }

            DescribeAutoScalingGroupsResult response = new DescribeAutoScalingGroupsResult();
            AutoScalingGroup autoScalingGroup = new AutoScalingGroup();

            if (currentTagState.get() != null) {
                TagDescription tag = new TagDescription();
                tag.setKey(TAG_MASTER_ENABLED);
                tag.setValue(currentTagState.get());

                autoScalingGroup.setTags(Collections.singletonList(tag));
            }

            response.setAutoScalingGroups(Collections.singletonList(autoScalingGroup));

            asyncHandler.onSuccess(request, response);

            invocationCounter.incrementAndGet();
            return Futures.immediateFuture(response);
        });

        resolver = new AsgLocalMasterReadinessResolver(
                configuration,
                autoScalingClient,
                REFRESH_SCHEDULER_DESCRIPTOR.toBuilder()
                        .withInitialDelay(Duration.ofMillis(0))
                        .withInterval(Duration.ofMillis(1))
                        .build(),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    @Test
    public void testResolve() {
        Iterator<ReadinessStatus> it = resolver.observeLocalMasterReadinessUpdates().toIterable().iterator();
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);

        // Change to NonLeader
        currentTagState.set("true");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);

        // Change to Inactive
        currentTagState.set("false");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);

        // Change to NonLeader
        currentTagState.set("true");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);

        // Remove tag
        currentTagState.set(null);
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);
    }

    @Test
    public void testInvalidTagValues() {
        Iterator<ReadinessStatus> it = resolver.observeLocalMasterReadinessUpdates().toIterable().iterator();
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);

        // Change to NonLeader
        currentTagState.set("true");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);

        // Set bad state, and wait until it is read
        currentTagState.set("bad_state");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);

        // Change to NonLeader to trigger update
        currentTagState.set("true");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);
    }

    @Test
    public void testAwsClientError() {
        Iterator<ReadinessStatus> it = resolver.observeLocalMasterReadinessUpdates().toIterable().iterator();
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);

        // Simulate error
        int currentCounter = invocationCounter.get();
        currentTagState.set(ERROR_MARKER);
        await().until(() -> invocationCounter.get() > currentCounter);

        // Change to NonLeader to trigger update
        currentTagState.set("true");
        assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);
    }
}