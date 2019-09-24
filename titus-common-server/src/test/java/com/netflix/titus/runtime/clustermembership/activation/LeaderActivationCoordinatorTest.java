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

package com.netflix.titus.runtime.clustermembership.activation;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipServiceStub;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;

public class LeaderActivationCoordinatorTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final LeaderActivationConfiguration configuration = Archaius2Ext.newConfiguration(
            LeaderActivationConfiguration.class,
            "leaderCheckIntervalMs", "1"
    );

    private final ClusterMembershipServiceStub membershipServiceStub = new ClusterMembershipServiceStub();

    private final TestableService testableService = new TestableService();
    private final AtomicReference<Throwable> errorCallback = new AtomicReference<>();

    private LeaderActivationCoordinator coordinator;

    @Before
    public void setUp() {
        this.coordinator = new LeaderActivationCoordinator(
                configuration,
                Collections.singletonList(testableService),
                errorCallback::set,
                membershipServiceStub,
                titusRuntime
        );
    }

    @After
    public void tearDown() {
        Evaluators.acceptNotNull(coordinator, LeaderActivationCoordinator::shutdown);
    }

    @Test(timeout = 30_000)
    public void testActivation() {
        await().until(() -> testableService.activationCount.get() > 0);
        membershipServiceStub.stopBeingLeader().block();
        await().until(() -> testableService.deactivationCount.get() > 0);
    }

    private class TestableService implements LeaderActivationListener {

        private final AtomicInteger activationCount = new AtomicInteger();
        private final AtomicInteger deactivationCount = new AtomicInteger();

        @Override
        public void activate() {
            activationCount.getAndIncrement();
        }

        @Override
        public void deactivate() {
            deactivationCount.getAndIncrement();
        }
    }
}