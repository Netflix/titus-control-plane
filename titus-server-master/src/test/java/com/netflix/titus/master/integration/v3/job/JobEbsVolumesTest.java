/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobEbsVolumeGenerator;
import com.netflix.titus.testkit.model.job.JobIpAllocationGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.master.integration.v3.job.JobTestUtils.submitBadJob;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;

@Category(IntegrationTest.class)
public class JobEbsVolumesTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = batchJobDescriptors(batchOfSizeAndEbsVolumes(1)).getValue();
    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = serviceJobDescriptors(serviceOfSizeAndEbsVolumes(1)).getValue();

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private static JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        client = titusStackResource.getMaster().getV3BlockingGrpcClient();
    }

    /**
     * Tests a service job with a single task and EBS volume.
     */
    @Test(timeout = 30_000)
    public void testServiceEbsVolumeConstraint() {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        // Expect tasks to have been assigned a volume
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(
                                TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID,
                                ONE_TASK_SERVICE_JOB.getContainer().getContainerResources().getEbsVolumes().get(0).getVolumeId())));
    }

    /**
     * Tests a batch job with a single task and EBS volume.
     */
    @Test(timeout = 30_000)
    public void testBatchEbsVolumeConstraint() {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        // Expect tasks to have been assigned a volume
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(
                                TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID,
                                ONE_TASK_BATCH_JOB.getContainer().getContainerResources().getEbsVolumes().get(0).getVolumeId())));
    }

    /**
     * Tests a service job with multiple tasks and EBS volumes.
     */
    @Test(timeout = 30_000)
    public void testMultiVolumeJob() {
        JobDescriptor<ServiceJobExt> serviceJobDescriptor = serviceJobDescriptors(serviceOfSizeAndEbsVolumes(4)).getValue();
        List<EbsVolume> ebsVolumes = serviceJobDescriptor.getContainer().getContainerResources().getEbsVolumes();

        jobsScenarioBuilder.schedule(serviceJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(0).getVolumeId()))
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(1).getVolumeId()))
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(2).getVolumeId()))
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(3).getVolumeId()))
        );
    }

    /**
     * Tests a new replacement task retains the same EBS volume as the previous task.
     */
    @Test(timeout = 30_000)
    public void testReplacementTaskEbsVolume() {
        JobDescriptor<ServiceJobExt> serviceJobDescriptor = serviceJobDescriptors(serviceOfSizeAndEbsVolumes(3)).getValue();
        List<EbsVolume> ebsVolumes = serviceJobDescriptor.getContainer().getContainerResources().getEbsVolumes();

        jobsScenarioBuilder.schedule(serviceJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        // Start the initial tasks
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(0).getVolumeId()))
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(1).getVolumeId()))
                        .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(2).getVolumeId()))
                        // Finish the initial tasks and make sure they are replaced
                        .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.transitionUntil(TaskStatus.TaskState.Finished))
                        .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskStatus.TaskState.Finished))
                        .inTask(1, taskScenarioBuilder -> taskScenarioBuilder.transitionUntil(TaskStatus.TaskState.Finished))
                        .inTask(1, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskStatus.TaskState.Finished))
                        .inTask(2, taskScenarioBuilder -> taskScenarioBuilder.transitionUntil(TaskStatus.TaskState.Finished))
                        .inTask(2, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskStatus.TaskState.Finished))
                        .expectAllTasksCreated()
                        .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                        .assertTasks(task -> task.get(0).getResubmitNumber() == 1)
                        .assertTasks(task -> task.get(1).getResubmitNumber() == 1)
                        .assertTasks(task -> task.get(2).getResubmitNumber() == 1)
                        .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.startTask()))
                        .inTask(1, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.startTask()))
                        .inTask(2, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.startTask()))
                        // Make sure replacements have the same volume as its resubmitOf
                        .expectTasksInSlot(0, sameEbsVolumePredicate())
                        .expectTasksInSlot(1, sameEbsVolumePredicate())
                        .expectTasksInSlot(2, sameEbsVolumePredicate())
        );
    }

    /**
     * Tests a batch job with size greater than EBS volumes cannot be created.
     */
    @Test(timeout = 30_000)
    public void testBatchJobInstanceValidation() {
        JobDescriptor<BatchJobExt> invalidJobDescriptor = ONE_TASK_BATCH_JOB
                .but(j -> j.getExtensions().toBuilder().withSize(2).build());

        submitBadJob(client,
                toGrpcJobDescriptor(invalidJobDescriptor),
                "container.containerResources.ebsVolumes");
    }

    /**
     * Tests a service job with max greater than EBS volumes cannot be created.
     */
    @Test(timeout = 30_000)
    public void testServiceJobInstanceValidation() {
        JobDescriptor<ServiceJobExt> invalidJobDescriptor = ONE_TASK_SERVICE_JOB
                .but(j -> j.getExtensions().toBuilder().withCapacity(
                        j.getExtensions().getCapacity().toBuilder().withMax(2).build()).build());

        submitBadJob(client,
                toGrpcJobDescriptor(invalidJobDescriptor),
                "container.containerResources.ebsVolumes");
    }

    /**
     * Tests a service job update with max greater than EBS volumes cannot be applied.
     */
    @Test(timeout = 30_000)
    public void testServiceJobUpdateValidation() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder ->
                // Schedule job with 1 EBS volume
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(
                                TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID,
                                ONE_TASK_SERVICE_JOB.getContainer().getContainerResources().getEbsVolumes().get(0).getVolumeId())
                        )
                        // Try and update max capacity to 2 and make sure it is rejected.
                        .updateJobCapacityMaxInvalid(2)
        );
    }

    /**
     * Tests that a job with EBS volumes and Static IPs are paired with resources in the expected order.
     */
    @Test(timeout = 30_000)
    public void testServiceJobWithStaticIps() {
        int size = 4;
        JobDescriptor<ServiceJobExt> serviceJobDescriptor = serviceJobDescriptors(serviceOfSizeAndEbsVolumes(size)).getValue();
        List<SignedIpAddressAllocation> ipAllocations = JobIpAllocationGenerator.jobIpAllocations(size).toList();
        JobDescriptor<ServiceJobExt> serviceJobDescriptorWithIps = JobFunctions.jobWithIpAllocations(serviceJobDescriptor, ipAllocations);
        List<EbsVolume> ebsVolumes = serviceJobDescriptorWithIps.getContainer().getContainerResources().getEbsVolumes();

        jobsScenarioBuilder.schedule(serviceJobDescriptorWithIps, jobScenarioBuilder ->
                jobScenarioBuilder
                        .expectSome(1, taskScenarioBuilder -> matchingEbsAndIpIndex(taskScenarioBuilder.getTask(), ebsVolumes, ipAllocations, 0))
                        .expectSome(1, taskScenarioBuilder -> matchingEbsAndIpIndex(taskScenarioBuilder.getTask(), ebsVolumes, ipAllocations, 1))
                        .expectSome(1, taskScenarioBuilder -> matchingEbsAndIpIndex(taskScenarioBuilder.getTask(), ebsVolumes, ipAllocations, 2))
                        .expectSome(1, taskScenarioBuilder -> matchingEbsAndIpIndex(taskScenarioBuilder.getTask(), ebsVolumes, ipAllocations, 3))
        );
    }

    private static Function<JobDescriptor<BatchJobExt>, JobDescriptor<BatchJobExt>> batchOfSizeAndEbsVolumes(int size) {
        List<EbsVolume> ebsVolumes = JobEbsVolumeGenerator.jobEbsVolumes(size).toList();
        Map<String, String> ebsVolumeAttributes = JobEbsVolumeGenerator.jobEbsVolumesToAttributes(ebsVolumes);
        return jd -> JobFunctions.changeBatchJobSize(jd, size)
                .but(jdb -> JobFunctions.jobWithEbsVolumes(jdb, ebsVolumes, ebsVolumeAttributes));
    }

    private static Function<JobDescriptor<ServiceJobExt>, JobDescriptor<ServiceJobExt>> serviceOfSizeAndEbsVolumes(int size) {
        List<EbsVolume> ebsVolumes = JobEbsVolumeGenerator.jobEbsVolumes(size).toList();
        Map<String, String> ebsVolumeAttributes = JobEbsVolumeGenerator.jobEbsVolumesToAttributes(ebsVolumes);
        return jd -> JobFunctions.changeServiceJobCapacity(jd, size)
                .but(jdb -> JobFunctions.jobWithEbsVolumes(jdb, ebsVolumes, ebsVolumeAttributes));
    }

    private static Predicate<List<TaskScenarioBuilder>> sameEbsVolumePredicate() {
        return taskScenarioBuilders -> taskScenarioBuilders.stream()
                .map(taskScenarioBuilder -> taskScenarioBuilder.getTask().getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID))
                .distinct()
                .count() == 1;
    }

    private boolean matchingEbsAndIpIndex(Task task, List<EbsVolume> ebsVolumes, List<SignedIpAddressAllocation> signedIpAddressAllocations, int index) {
        return task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, "").equals(ebsVolumes.get(index).getVolumeId())
                && task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID, "").equals(signedIpAddressAllocations.get(index).getIpAddressAllocation().getAllocationId());
    }
}
