/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation;

import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;

public abstract class AbstractTaskRelocationTest {

    protected final TitusRuntime titusRuntime;
    protected final TestClock clock;

    protected final RelocationConfiguration configuration = Archaius2Ext.newConfiguration(RelocationConfiguration.class);

    protected final KubeApiFacade kubeApiFacade = TestDataFactory.mockKubeApiFacade();

    protected final RelocationConnectorStubs relocationConnectorStubs;

    protected final NodeDataResolver nodeDataResolver;
    protected final ReadOnlyJobOperations jobOperations;

    protected final ReadOnlyEvictionOperations evictionOperations;
    protected final EvictionServiceClient evictionServiceClient;

    protected AbstractTaskRelocationTest(RelocationConnectorStubs relocationConnectorStubs) {
        this.relocationConnectorStubs = relocationConnectorStubs;
        this.titusRuntime = relocationConnectorStubs.getTitusRuntime();
        this.clock = (TestClock) titusRuntime.getClock();

        this.nodeDataResolver = relocationConnectorStubs.getNodeDataResolver();
        this.jobOperations = relocationConnectorStubs.getJobOperations();
        this.evictionOperations = relocationConnectorStubs.getEvictionOperations();
        this.evictionServiceClient = relocationConnectorStubs.getEvictionServiceClient();
    }
}
