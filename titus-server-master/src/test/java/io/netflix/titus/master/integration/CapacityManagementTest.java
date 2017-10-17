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

package io.netflix.titus.master.integration;

import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.data.core.ApplicationSlaSample;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.master.endpoint.v2.rest.Representation2ModelConvertions.asRepresentation;
import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collection of integration tests for application SLA management. These tests are driven by the application SLA REST
 * API, and validate proper interaction with the storage layer and cloud provider (auto-scale actions).
 */
@Category(IntegrationTest.class)
public class CapacityManagementTest extends BaseIntegrationTest {

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withCriticalTier(0.1, AwsInstanceType.M3_XLARGE)
                    .withFlexTier(0.1, AwsInstanceType.M3_2XLARGE, AwsInstanceType.G2_2XLarge)
                    .withAgentCluster(aTitusAgentCluster("agentClusterOne", 0).withSize(2).withInstanceType(AwsInstanceType.M3_XLARGE))
                    .withAgentCluster(aTitusAgentCluster("agentClusterTwo", 1).withSize(2).withInstanceType(AwsInstanceType.M3_2XLARGE))
                    .build()
    );

    private TitusMasterClient client;
    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;

    @Before
    public void setUp() throws Exception {
        EmbeddedTitusMaster titusMaster = titusMasterResource.getMaster();

        client = titusMaster.getClient();

        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
    }

    /**
     * Add new critical tier SLA, which should be persisted in a storage, and should force a scale up of
     * a server group.
     */
    @Test(timeout = 30000)
    public void addCriticalTierJobSla() throws Exception {
        ApplicationSLA applicationSLA = ApplicationSlaSample.CriticalLarge.build();

        String location = client.addApplicationSLA(asRepresentation(applicationSLA)).toBlocking().first();
        assertThat(location).contains("/api/v2/management/applications/" + applicationSLA.getAppName());
    }
}
