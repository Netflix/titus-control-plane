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

package io.netflix.titus.master.integration.v3.job;

import java.util.List;

import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import io.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import io.netflix.titus.runtime.store.v3.memory.InMemoryJobStore;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStack;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import io.netflix.titus.testkit.model.job.ContainersGenerator;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.master.integration.v3.scenario.JobAsserts.containerWithEfsMounts;
import static io.netflix.titus.master.integration.v3.scenario.JobAsserts.containerWithResources;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.completeTask;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.jobFinished;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.killJob;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static java.util.Arrays.asList;

/**
 */
@Category(IntegrationTest.class)
public class JobSubmitAndControlBasicTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder().withApplicationName("myApp").build();
    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = oneTaskServiceJobDescriptor().toBuilder().withApplicationName("myApp").build();

    private final JobStore store = new InMemoryJobStore();

    @Rule
    public final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusStack.aTitusStack()
            .withMaster(EmbeddedTitusMaster.testTitusMaster()
                    .withProperty("titus.master.grpcServer.v3EnabledApps", "myApp")
                    .withProperty("titusMaster.jobManager.launchedTimeoutMs", "3000")
                    .withCriticalTier(0.1, AwsInstanceType.M3_XLARGE)
                    .withFlexTier(0.1, AwsInstanceType.M3_2XLARGE, AwsInstanceType.G2_2XLarge)
                    .withAgentCluster(aTitusAgentCluster("agentClusterOne", 0).withSize(2).withInstanceType(AwsInstanceType.M3_XLARGE))
                    .withAgentCluster(aTitusAgentCluster("agentClusterTwo", 1).withSize(2).withInstanceType(AwsInstanceType.M3_2XLARGE))
                    .withJobStore(store)
                    .build())
            .withDefaultGateway()
            .build()
    );

    private EmbeddedTitusMaster titusMaster;

    private JobsScenarioBuilder jobsScenarioBuilder;

    @Before
    public void setUp() throws Exception {
        titusMaster = titusStackResource.getMaster();
        jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource.getStack().getTitusOperations());
    }

    /**
     * Verify batch job submit with the expected state transitions. Verify agent receives proper resources.
     */
    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobWhichEndsOk() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .assertEachContainer(
                        containerWithResources(ONE_TASK_BATCH_JOB.getContainer().getContainerResources()),
                        "Container not assigned the expected amount of resources"
                )
                .allTasks(completeTask())
                .template(jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    /**
     * Verify batch job submit with the expected state transitions. Verify agent receives proper EFS mount data.
     */
    @Test(timeout = 30_000)
    public void testSubmitBatchJobWithEfsMount() throws Exception {
        EfsMount efsMount1 = ContainersGenerator.efsMounts().getValue().toBuilder().withMountPoint("/data/logs").build();
        EfsMount efsMount2 = ContainersGenerator.efsMounts().skip(1).getValue().toBuilder().withMountPoint("/data").build();
        List<EfsMount> efsMounts = asList(efsMount1, efsMount2);
        List<EfsMount> expectedOrder = asList(efsMount2, efsMount1);

        JobDescriptor<BatchJobExt> jobWithEfs = ONE_TASK_BATCH_JOB.but(jd -> jd.getContainer().but(c -> c.getContainerResources().toBuilder().withEfsMounts(efsMounts)));
        jobsScenarioBuilder.schedule(jobWithEfs, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .assertEachContainer(
                        containerWithEfsMounts(expectedOrder),
                        "Container not assigned the expected EFS mount"
                )
                .allTasks(completeTask())
                .template(jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testJobSubmitWithNoSecurityProfile() throws Exception {
        JobDescriptor<BatchJobExt> jobWithNoSecurityProfile = ONE_TASK_BATCH_JOB.but(j -> j.getContainer().but(c -> SecurityProfile.newBuilder().build()));
        jobsScenarioBuilder.schedule(jobWithNoSecurityProfile, jobScenarioBuilder -> jobScenarioBuilder
                .assertJob(job -> {
                            JobConfiguration jobConfiguration = titusMaster.getInstance(JobConfiguration.class);

                            SecurityProfile securityProfile = job.getJobDescriptor().getContainer().getSecurityProfile();
                            if (!securityProfile.getIamRole().equals(jobConfiguration.getDefaultIamRole())) {
                                return false;
                            }
                            if (!securityProfile.getSecurityGroups().equals(jobConfiguration.getDefaultSecurityGroups())) {
                                return false;
                            }
                            return true;
                        }
                )
                .killJob()
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobWhichFails() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.transitionTo(Protos.TaskState.TASK_FAILED))
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.Finished))
                .template(jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobAndKillTask() throws Exception {
        JobDescriptor<BatchJobExt> retryableJob = ONE_TASK_BATCH_JOB.but(jd -> jd.getExtensions().toBuilder()
                .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(3).build())
        );
        jobsScenarioBuilder.schedule(retryableJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .allTasks(TaskScenarioBuilder::killTask)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.KillInitiated, TaskState.Finished))
                .template(jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobAndKillIt() throws Exception {
        JobDescriptor<BatchJobExt> retryableJob = ONE_TASK_BATCH_JOB.but(jd -> jd.getExtensions().toBuilder()
                .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(3).build())
        );
        jobsScenarioBuilder.schedule(retryableJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .template(killJob())
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobWithNotRunningTaskAndKillIt() throws Exception {
        JobDescriptor<BatchJobExt> queuedJob = ONE_TASK_BATCH_JOB.but(jd -> jd.getContainer().but(
                c -> c.getContainerResources().toBuilder().withCpu(64) // Prevent it from being scheduled
        ));
        jobsScenarioBuilder.schedule(queuedJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Accepted))
                .template(killJob())
        );
    }

    @Test(timeout = 30_000)
    public void submitGpuBatchJob() throws Exception {
        titusMaster.addAgentCluster(aTitusAgentCluster("gpuAgentCluster", 3).withInstanceType(AwsInstanceType.G2_2XLarge));

        JobDescriptor<BatchJobExt> gpuJobDescriptor =
                ONE_TASK_BATCH_JOB.but(j -> j.getContainer().but(c -> c.getContainerResources().toBuilder().withGpu(1)));

        jobsScenarioBuilder.schedule(gpuJobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectInstanceType(AwsInstanceType.G2_2XLarge))
        );
    }

    /**
     * Verify service job submit with the expected state transitions.
     */
    @Test(timeout = 30_000)
    public void testSubmitSimpleServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .template(killJob())
        );
    }

    @Test(timeout = 30_000)
    public void testEnableDisableServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .updateJobStatus(true)
                .updateJobStatus(false)
        );
    }
}
