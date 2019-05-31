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

package com.netflix.titus.api.supervisor.service.resolver;

import java.util.Arrays;
import java.util.Iterator;

import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.EmitterProcessor;

import static org.mockito.Mockito.when;

public class AggregatingLocalMasterReadinessResolverTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final LocalMasterReadinessResolver delegate1 = Mockito.mock(LocalMasterReadinessResolver.class);
    private final LocalMasterReadinessResolver delegate2 = Mockito.mock(LocalMasterReadinessResolver.class);

    private final EmitterProcessor<ReadinessStatus> delegate1Processor = EmitterProcessor.create(1);
    private final EmitterProcessor<ReadinessStatus> delegate2Processor = EmitterProcessor.create(1);

    private AggregatingLocalMasterReadinessResolver resolver;

    @Before
    public void setUp() {
        when(delegate1.observeLocalMasterReadinessUpdates()).thenReturn(delegate1Processor);
        when(delegate2.observeLocalMasterReadinessUpdates()).thenReturn(delegate2Processor);
        resolver = new AggregatingLocalMasterReadinessResolver(Arrays.asList(delegate1, delegate2), titusRuntime);
    }

    @Test(timeout = 30_000)
    public void testAggregation() {
        Iterator<ReadinessStatus> it = resolver.observeLocalMasterReadinessUpdates().toIterable().iterator();

        // Enabled + Disabled == Disabled
        delegate1Processor.onNext(newStatus(ReadinessState.Enabled));
        delegate2Processor.onNext(newStatus(ReadinessState.Disabled));
        Assertions.assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);

        // Enabled + Enabled == Enabled
        delegate2Processor.onNext(newStatus(ReadinessState.Enabled));
        Assertions.assertThat(it.next().getState()).isEqualTo(ReadinessState.Enabled);

        // Disabled + Enabled == Disabled
        delegate1Processor.onNext(newStatus(ReadinessState.Disabled));
        Assertions.assertThat(it.next().getState()).isEqualTo(ReadinessState.Disabled);
    }

    private ReadinessStatus newStatus(ReadinessState state) {
        return ReadinessStatus.newBuilder().withState(state).build();
    }
}