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

package io.netflix.titus.master.integration.v3;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.runtime.store.v3.memory.InMemoryJobStore;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.master.integration.v3.scenario.JobAsserts.jobInState;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

@Category(IntegrationTest.class)
public class V3JobSchedulingTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder().withApplicationName("myApp").build();

    private final JobStore titusStore = new InMemoryJobStore();

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withProperty("mantis.worker.state.launched.timeout.millis", "100")
                    .withProperty("mantis.master.grpcServer.v3EnabledApps", "myApp")
                    .withProperty("titusMaster.jobManager.launchedTimeoutMs", "3000")
                    .withCriticalTier(0.1, AwsInstanceType.M3_XLARGE)
                    .withFlexTier(0.1, AwsInstanceType.M3_2XLARGE, AwsInstanceType.G2_2XLarge)
                    .withAgentCluster(aTitusAgentCluster("agentClusterOne", 0).withSize(2).withInstanceType(AwsInstanceType.M3_XLARGE))
                    .withAgentCluster(aTitusAgentCluster("agentClusterTwo", 1).withSize(2).withInstanceType(AwsInstanceType.M3_2XLARGE))
                    .withJobStore(titusStore)
                    .build()
    );

    private EmbeddedTitusMaster titusMaster;

    private JobsScenarioBuilder jobsScenarioBuilder;

    @Before
    public void setUp() throws Exception {
        titusMaster = titusMasterResource.getMaster();
        jobsScenarioBuilder = new JobsScenarioBuilder(titusMasterResource.getOperations());
    }

    /**
     * Verify batch job submission for two agent clusters with identical fitness, but only one having required
     * resources.
     * TODO We should add second cluster in this test, but as adding cluster requires master restart, we provide two clusters in the initialization step
     */
    @Test(timeout = 30_000)
    public void submitBatchJobWhenTwoAgentClustersWithSameFitnessButDifferentResourceAmounts() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor =
                ONE_TASK_BATCH_JOB.but(j -> j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(7)));

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectInstanceType(AwsInstanceType.M3_2XLARGE))
        );
    }

    @Test(timeout = 30_000)
    public void submitBatchJobAndRebootTitusMaster() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
        );

        jobsScenarioBuilder.stop();
        titusMaster.reboot();
        jobsScenarioBuilder = new JobsScenarioBuilder(titusMasterResource.getOperations());

        jobsScenarioBuilder
                .assertJobs(jobs -> jobs.size() == 1)
                .takeJob(0)
                .assertJob(jobInState(JobState.Accepted))
                .assertTasks(tasks -> tasks.size() == 1);
    }
}
