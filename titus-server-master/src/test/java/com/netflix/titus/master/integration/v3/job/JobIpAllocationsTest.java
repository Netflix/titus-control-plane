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

package com.netflix.titus.master.integration.v3.job;

import java.util.function.Function;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobIpAllocationGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;

@Category(IntegrationTest.class)
public class JobIpAllocationsTest extends BaseIntegrationTest {
    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = batchJobDescriptors(batchOfSizeAndIps(1)).getValue();
    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = serviceJobDescriptors(serviceOfSizeAndIps(1)).getValue();

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
    }

    /**
     * Tests a service job with a single task and IP assignment.
     */
    @Test(timeout = 30_000)
    public void testServiceIpAllocationConstraint() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        // Expect tasks to have been assigned
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, getIpAllocationIdFromJob(0, ONE_TASK_SERVICE_JOB)))
                        // Expect tasks to be assigned to correct AZ
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(getZoneFromJobIpAllocation(0, ONE_TASK_SERVICE_JOB))));
    }

    /**
     * Tests a batch job with a single task and IP assignment.
     */
    @Test(timeout = 30_000)
    public void testBatchIpAllocationConstraint() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        // Expect tasks to have been assigned
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, getIpAllocationIdFromJob(0, ONE_TASK_BATCH_JOB)))
                        // Expect tasks to be assigned to correct AZ
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(getZoneFromJobIpAllocation(0, ONE_TASK_BATCH_JOB))));
    }

    /**
     * Tests that a job waits for an assigned IP allocation to be freed before being scheduled.
     */
    @Test(timeout = 30_000)
    public void testAlreadyAssignedIpAllocationConstraint() throws Exception {
        JobDescriptor<ServiceJobExt> firstIpJobDescriptor = ONE_TASK_SERVICE_JOB;
        JobDescriptor<ServiceJobExt> secondIpJobDescriptor = firstIpJobDescriptor.but(j -> j.getJobGroupInfo().toBuilder().withSequence("v001"));

        // Schedule the first task and ensure it's in the correct zone with the correct task context
        jobsScenarioBuilder.schedule(firstIpJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, getIpAllocationIdFromJob(0, firstIpJobDescriptor)))
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(getZoneFromJobIpAllocation(0, firstIpJobDescriptor))));

        // Schedule the second task and ensure it's blocked on the first task
        jobsScenarioBuilder.schedule(secondIpJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.jobAccepted())
                        .expectAllTasksCreated()
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskStatus.TaskState.Accepted)));

        // Terminate the first job
        jobsScenarioBuilder
                .takeJob(0)
                .template(ScenarioTemplates.killJob());

        // Ensure the second job's task starts and is placed successfully
        jobsScenarioBuilder
                .takeJob(1)
                .template(ScenarioTemplates.startTasks())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, getIpAllocationIdFromJob(0, secondIpJobDescriptor)))
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(getZoneFromJobIpAllocation(0, secondIpJobDescriptor)));
    }

    /**
     * Tests a service job with multiple IP assignments.
     */
    @Test(timeout = 30_000)
    public void testMultiTaskIpAllocations() throws Exception {
        JobDescriptor<ServiceJobExt> serviceJobExtJobDescriptor = serviceJobDescriptors(serviceOfSizeAndIps(2)).getValue();

        jobsScenarioBuilder.schedule(serviceJobExtJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, "").equals(getIpAllocationIdFromJob(0, serviceJobExtJobDescriptor)))
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, "").equals(getIpAllocationIdFromJob(1, serviceJobExtJobDescriptor)))
        );
    }

    /**
     * Tests a new replacement task retains the same IP allocation as the previous task.
     */
    @Test
    public void testReplacementTaskIpAllocation() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder ->
                jobScenarioBuilder
                        // Start the initial task
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, getIpAllocationIdFromJob(0, ONE_TASK_SERVICE_JOB)))
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(getZoneFromJobIpAllocation(0, ONE_TASK_SERVICE_JOB)))
                        // Finish the initial task and make sure it is replaced
                        .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.transitionUntil(TaskStatus.TaskState.Finished))
                        .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskStatus.TaskState.Finished))
                        .expectAllTasksCreated()
                        .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                        .assertTasks(task -> task.get(0).getResubmitNumber() == 1)
                        .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.startTask()))
                        // Make sure replacement has correct attribute and placement
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, getIpAllocationIdFromJob(0, ONE_TASK_SERVICE_JOB)))
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(getZoneFromJobIpAllocation(0, ONE_TASK_SERVICE_JOB)))
        );
    }

    private static Function<JobDescriptor<BatchJobExt>, JobDescriptor<BatchJobExt>> batchOfSizeAndIps(int size) {
        return jd -> JobFunctions.changeBatchJobSize(jd, size)
                .but(j -> j.getContainer()
                        .but(c -> c.getContainerResources().toBuilder().withSignedIpAddressAllocations(JobIpAllocationGenerator.jobIpAllocations(size).getValues(size)).build()));
    }

    private static Function<JobDescriptor<ServiceJobExt>, JobDescriptor<ServiceJobExt>> serviceOfSizeAndIps(int size) {
        return jd -> JobFunctions.changeServiceJobCapacity(jd, size)
                .but(j -> j.getContainer()
                        .but(c -> c.getContainerResources().toBuilder().withSignedIpAddressAllocations(JobIpAllocationGenerator.jobIpAllocations(size).getValues(size)).build()));
    }

    private static String getIpAllocationIdFromJob(int idx, JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.getContainer().getContainerResources().getSignedIpAddressAllocations().get(idx).getIpAddressAllocation().getAllocationId();
    }

    private static String getZoneFromJobIpAllocation(int idx, JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.getContainer().getContainerResources().getSignedIpAddressAllocations().get(idx).getIpAddressAllocation().getIpAddressLocation().getAvailabilityZone();
    }
}
