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

import java.util.Iterator;

import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.testkit.model.supervisor.MasterInstanceGenerator;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.EmitterProcessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultLocalMasterInstanceResolverTest {

    private final LocalMasterReadinessResolver localMasterReadinessResolver = mock(LocalMasterReadinessResolver.class);

    private MasterInstance initial = MasterInstanceGenerator.masterInstances(MasterState.Starting, "testId").getValue();

    private DefaultLocalMasterInstanceResolver resolver;

    private final EmitterProcessor<ReadinessStatus> readinessUpdatesProcessor = EmitterProcessor.create(1);

    @Before
    public void setUp() throws Exception {
        when(localMasterReadinessResolver.observeLocalMasterReadinessUpdates()).thenReturn(readinessUpdatesProcessor);
        resolver = new DefaultLocalMasterInstanceResolver(localMasterReadinessResolver, initial);
    }

    @Test(timeout = 30_000)
    public void testResolver() {
        Iterator<MasterInstance> it = resolver.observeLocalMasterInstanceUpdates().toIterable().iterator();

        readinessUpdatesProcessor.onNext(newReadinessStatus(ReadinessState.NotReady));
        assertThat(it.next().getStatus().getState()).isEqualTo(MasterState.Starting);

        readinessUpdatesProcessor.onNext(newReadinessStatus(ReadinessState.Enabled));
        assertThat(it.next().getStatus().getState()).isEqualTo(MasterState.NonLeader);
    }

    private ReadinessStatus newReadinessStatus(ReadinessState state) {
        return ReadinessStatus.newBuilder()
                .withState(state)
                .withMessage("Test")
                .build();
    }
}