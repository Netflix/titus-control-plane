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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import io.netflix.titus.testkit.embedded.master.EmbeddedStorageProvider;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.JobObserver;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.activate;
import static io.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters.basicMaster;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class ResourceSchedulingTest extends BaseIntegrationTest {

    private final TitusMasterResource titusMasterResource = new TitusMasterResource(
            basicMaster(new SimulatedCloud().createAgentInstanceGroups(
                    new SimulatedAgentGroupDescriptor("flex1", AwsInstanceType.M3_2XLARGE.name(), 0, 1, 1, 2)
            )).toBuilder()
                    .withProperty("titus.scheduler.globalTaskLaunchingConstraintEvaluatorEnabled", "false")
                    .build()
    );

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusMasterResource);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(titusMasterResource).around(instanceGroupsScenarioBuilder);

    private EmbeddedTitusMaster titusMaster;

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator(UUID.randomUUID().toString());

    private TitusMasterClient client;
    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(activate("flex1"));

        titusMaster = titusMasterResource.getMaster();
        client = titusMaster.getClient();
        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
    }

    /**
     * Verify ENI assignment
     */
    @Test(timeout = 30_000)
    public void checkIpPerEniLimitIsPreserved() throws Exception {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
                .instancesMin(1).instancesDesired(3).instancesMax(10)
                .allocateIpAddress(true)
                .restartOnSuccess(true)
                .retries(2)
                .build();

        String jobId = runServiceJob(jobSpec);

        // We need to examine internal data structure to check ENI assignments
        Collection<V2WorkerMetadata> tasksMetadata = ((EmbeddedStorageProvider) titusMaster.getStorageProvider()).getJob(jobId)
                .getStageMetadata(1)
                .getAllWorkers();

        List<String> eniIDs = tasksMetadata.stream().map(t -> t.getTwoLevelResources().get(0).getLabel()).collect(Collectors.toList());
        assertThat(eniIDs).contains("0", "1");
    }

    private String runServiceJob(TitusJobSpec jobSpec) throws InterruptedException {
        String jobId = client.submitJob(jobSpec).toBlocking().first();

        int count = jobSpec.getInstancesDesired();
        List<TaskExecutorHolder> taskHolders = taskExecutorHolders.takeNextOrWait(count);
        for (TaskExecutorHolder taskHolder : taskHolders) {
            assertThat(taskHolder.getJobId()).isEqualTo(jobId);
            taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
            taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
        }
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.awaitJobInState(TitusTaskState.RUNNING);

        return jobId;
    }
}