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

package com.netflix.titus.master.supervisor.service.resolver;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import com.netflix.runtime.health.api.HealthCheckAggregator;
import com.netflix.runtime.health.api.HealthCheckStatus;
import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.junit.Before;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.master.supervisor.service.resolver.HealthLocalMasterReadinessResolver.REFRESH_SCHEDULER_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthLocalMasterReadinessResolverTest {

    private static final HealthCheckStatus HEALTHY = new HealthCheckStatus(true, Collections.emptyList());
    private static final HealthCheckStatus UNHEALTHY = new HealthCheckStatus(false, Collections.emptyList());

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final HealthCheckAggregator healthcheck = mock(HealthCheckAggregator.class);

    private HealthLocalMasterReadinessResolver resolver;

    private volatile HealthCheckStatus currentHealthStatus = HEALTHY;
    private volatile Throwable simulatedError;
    private volatile int invocationCounter;

    @Before
    public void setUp() throws Exception {
        when(healthcheck.check()).thenAnswer(invocation -> {
            CompletableFuture<HealthCheckStatus> future = new CompletableFuture<>();
            if (simulatedError != null) {
                future.completeExceptionally(new RuntimeException("simulated error"));
            } else if (currentHealthStatus != null) {
                future.complete(currentHealthStatus);
            } // else never complete
            invocationCounter++;
            return future;
        });

        this.resolver = new HealthLocalMasterReadinessResolver(
                healthcheck,
                REFRESH_SCHEDULER_DESCRIPTOR.toBuilder()
                        .withInterval(Duration.ofMillis(1))
                        .withTimeout(Duration.ofMillis(1))
                        .build(),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    @Test(timeout = 30_000)
    public void testResolve() {
        Iterator<ReadinessStatus> it = newStreamInitiallyHealthy();

        // Change to unhealthy
        currentHealthStatus = UNHEALTHY;
        await().until(() -> it.next().getState() == ReadinessState.Disabled);

        // Back to healthy
        currentHealthStatus = HEALTHY;
        await().until(() -> it.next().getState() == ReadinessState.Enabled);
    }

    @Test(timeout = 30_000)
    public void testError() {
        Iterator<ReadinessStatus> it = newStreamInitiallyHealthy();

        // Simulate error (no change expected)
        simulatedError = new RuntimeException("simulated error");
        int currentCounter = invocationCounter;
        await().until(() -> invocationCounter > currentCounter);

        // Change state
        currentHealthStatus = UNHEALTHY;
        simulatedError = null;
        await().until(() -> it.next().getState() == ReadinessState.Disabled);
    }

    @Test(timeout = 30_000)
    public void testTimeout() {
        Iterator<ReadinessStatus> it = newStreamInitiallyHealthy();

        // Trigger timeout
        currentHealthStatus = null;
        int currentCounter = invocationCounter;
        await().until(() -> invocationCounter > (currentCounter + 1));

        // Change state
        currentHealthStatus = UNHEALTHY;
        await().until(() -> it.next().getState() == ReadinessState.Disabled);
    }

    private Iterator<ReadinessStatus> newStreamInitiallyHealthy() {
        // Starts healthy
        Iterator<ReadinessStatus> it = resolver.observeLocalMasterReadinessUpdates().toIterable().iterator();
        ReadinessState first = it.next().getState();
        if (first == ReadinessState.NotReady) {
            assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);
        } else {
            assertThat(first).isEqualTo(ReadinessState.Enabled);
        }
        return it;
    }
}