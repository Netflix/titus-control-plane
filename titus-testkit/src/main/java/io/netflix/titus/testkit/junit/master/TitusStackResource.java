/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.junit.master;

import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.runtime.store.v3.memory.InMemoryJobStore;
import io.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import io.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStack;
import org.junit.rules.ExternalResource;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;

public class TitusStackResource extends ExternalResource {

    public static String V2_ENGINE_APP_PREFIX = "v2App";
    public static String V3_ENGINE_APP_PREFIX = "v3App";

    private final EmbeddedTitusStack embeddedTitusStack;

    public TitusStackResource(EmbeddedTitusStack embeddedTitusStack) {
        this.embeddedTitusStack = embeddedTitusStack;
    }

    @Override
    public void before() throws Throwable {
        embeddedTitusStack.boot();
    }

    @Override
    public void after() {
        embeddedTitusStack.shutdown();
    }

    public EmbeddedTitusStack getStack() {
        return embeddedTitusStack;
    }

    public EmbeddedTitusMaster getMaster() {
        return embeddedTitusStack.getMaster();
    }

    public EmbeddedTitusGateway getGateway() {
        return embeddedTitusStack.getGateway();
    }

    public EmbeddedTitusOperations getOperations() {
        return embeddedTitusStack.getTitusOperations();
    }

    public static TitusStackResource aDefaultStack() {
        JobStore store = new InMemoryJobStore();
        return new TitusStackResource(
                EmbeddedTitusStack.aTitusStack()
                        .withMaster(EmbeddedTitusMaster.testTitusMaster()
                                .withProperty("mantis.worker.state.launched.timeout.millis", "30000")
                                .withProperty("mantis.master.grpcServer.v3EnabledApps", String.format("(%s.*)", V3_ENGINE_APP_PREFIX))
                                .withProperty("titusMaster.jobManager.launchedTimeoutMs", "30000")
                                .withCriticalTier(0.1, AwsInstanceType.M3_XLARGE)
                                .withFlexTier(0.1, AwsInstanceType.M3_2XLARGE, AwsInstanceType.G2_2XLarge)
                                .withAgentCluster(aTitusAgentCluster("agentClusterOne", 0).withSize(3).withInstanceType(AwsInstanceType.M3_XLARGE))
                                .withAgentCluster(aTitusAgentCluster("agentClusterTwo", 1).withSize(3).withInstanceType(AwsInstanceType.M3_2XLARGE))
                                .withJobStore(store)
                                .build()
                        )
                        .withGateway(EmbeddedTitusGateway.aDefaultTitusGateway()
                                .withStore(store)
                                .build()
                        )
                        .build()
        );
    }
}
