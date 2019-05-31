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
import java.util.function.Supplier;

import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class OnOffLocalMasterReadinessResolverTest {

    private static final ReadinessStatus TURNED_OFF = ReadinessStatus.newBuilder().withState(ReadinessState.Disabled).withMessage("Turned off").build();

    private static final Duration CHECK_INTERVAL = Duration.ofMillis(1);

    private LocalMasterReadinessResolver delegate = Mockito.mock(LocalMasterReadinessResolver.class);

    private volatile boolean currentIsOn = true;
    private Supplier<Boolean> isOnSupplier = () -> currentIsOn;

    private Supplier<ReadinessStatus> enforcedStatusSupplier = () -> TURNED_OFF;

    private final EmitterProcessor<ReadinessStatus> delegateUpdatesProcessor = EmitterProcessor.create();

    @Before
    public void setUp() throws Exception {
        when(delegate.observeLocalMasterReadinessUpdates()).thenReturn(delegateUpdatesProcessor);
    }

    private Flux<ReadinessStatus> newResolverStream() {
        return new OnOffLocalMasterReadinessResolver(delegate, isOnSupplier, enforcedStatusSupplier, CHECK_INTERVAL, Schedulers.parallel()).observeLocalMasterReadinessUpdates();
    }

    @Test
    public void testOverride() {
        StepVerifier.withVirtualTime(this::newResolverStream)
                .then(() -> emitDelegateStateUpdate(ReadinessState.NotReady))
                .assertNext(status -> assertThat(status.getState()).isEqualTo(ReadinessState.NotReady))

                // Enable
                .then(() -> emitDelegateStateUpdate(ReadinessState.Enabled))
                .assertNext(status -> assertThat(status.getState()).isEqualTo(ReadinessState.Enabled))

                // Turn off
                .then(() -> currentIsOn = false)
                .thenAwait(CHECK_INTERVAL)
                .assertNext(status -> assertThat(status.getState()).isEqualTo(ReadinessState.Disabled))

                // Turn back on
                .then(() -> currentIsOn = true)
                .thenAwait(CHECK_INTERVAL)
                .assertNext(status -> assertThat(status.getState()).isEqualTo(ReadinessState.Enabled))

                .thenCancel()
                .verify();
    }

    private void emitDelegateStateUpdate(ReadinessState state) {
        delegateUpdatesProcessor.onNext(ReadinessStatus.newBuilder().withState(state).build());
    }
}