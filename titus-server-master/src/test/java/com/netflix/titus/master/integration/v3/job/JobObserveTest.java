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

package com.netflix.titus.master.integration.v3.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.JobStatus.JobState;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase.SERVICE;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.killJob;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class JobObserveTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusCells.basicCell(4));

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private JobManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        this.client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeJobs() {
        Iterator<JobChangeNotification> eventId = client.observeJobs(ObserveJobsQuery.getDefaultInstance());

        for (int i = 0; i < 2; i++) {
            String jobId = jobsScenarioBuilder.scheduleAndReturnJob(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder
                    .template(startTasksInNewJob())
                    .killJob()
            ).getId();

            JobChangeNotification event;
            while (true) {
                assertThat(eventId.hasNext()).describedAs("More events expected").isTrue();
                event = eventId.next();
                if (event.hasJobUpdate()) {
                    Job job = event.getJobUpdate().getJob();
                    assertThat(job.getId()).isEqualTo(jobId);
                    CellAssertions.assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME);
                    if (job.getStatus().getState() == JobState.Finished) {
                        break;
                    }
                }
            }
        }
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeSnapshotWithFilter() {
        startAll(
                batchJobDescriptors().getValue().toBuilder()
                        .withApplicationName("myApp")
                        .build(),
                batchJobDescriptors().getValue().toBuilder()
                        .withApplicationName("otherApp")
                        .build()
        );
        String myAppJobId = jobsScenarioBuilder.takeJobId(0);

        // start the stream after tasks are already running
        ObserveJobsQuery query = ObserveJobsQuery.newBuilder().putFilteringCriteria("applicationName", "myApp").build();
        Iterator<JobChangeNotification> eventIt = client.observeJobs(query);

        List<JobChangeNotification> events = new ArrayList<>();
        while (eventIt.hasNext()) {
            JobChangeNotification event = eventIt.next();
            if (event.hasJobUpdate()) {
                assertThat(event.getJobUpdate().getJob().getJobDescriptor().getApplicationName()).isEqualTo("myApp");
            } else if (event.hasTaskUpdate()) {
                assertThat(event.getTaskUpdate().getTask().getJobId()).isEqualTo(myAppJobId);
            } else if (event.hasSnapshotEnd()) {
                events.add(event);
                break;
            }
            events.add(event);
        }
        assertThat(events).hasSize(3);
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeByJobDescriptor() {
        startAll(
                batchJobDescriptors().getValue().toBuilder()
                        .withApplicationName("myApp")
                        .build(),
                batchJobDescriptors().getValue().toBuilder()
                        .withApplicationName("otherApp")
                        .withOwner(Owner.newBuilder().withTeamEmail("me@netflix.com").build())
                        .build(),
                batchJobDescriptors().getValue().but(j -> j.getContainer().toBuilder().withImage(
                        JobModel.newImage().withName("some/image").withTag("stable").build()
                )),
                batchJobDescriptors().getValue().toBuilder()
                        .withAttributes(ImmutableMap.<String, String>builder()
                                .put("attr1", "value1")
                                .put("attr2", "value2")
                                .build())
                        .build()
        );
        startAll(serviceJobDescriptors().getValue());

        observeByJobDescriptor(
                jobsScenarioBuilder.takeJobId(0),
                ObserveJobsQuery.newBuilder().putFilteringCriteria("applicationName", "myApp").build(),
                job -> assertThat(job.getJobDescriptor().getApplicationName()).isEqualTo("myApp")
        );
        observeByJobDescriptor(
                jobsScenarioBuilder.takeJobId(1),
                ObserveJobsQuery.newBuilder().putFilteringCriteria("owner", "me@netflix.com").build(),
                job -> assertThat(job.getJobDescriptor().getOwner().getTeamEmail()).isEqualTo("me@netflix.com")
        );
        observeByJobDescriptor(
                jobsScenarioBuilder.takeJobId(2),
                ObserveJobsQuery.newBuilder().putFilteringCriteria("imageName", "some/image").putFilteringCriteria("imageTag", "stable").build(),
                job -> {
                    Image image = job.getJobDescriptor().getContainer().getImage();
                    assertThat(image.getName()).isEqualTo("some/image");
                    assertThat(image.getTag()).isEqualTo("stable");
                }
        );
        observeByJobDescriptor(
                jobsScenarioBuilder.takeJobId(3),
                ObserveJobsQuery.newBuilder().putFilteringCriteria("attributes", "attr1,attr2:value2").putFilteringCriteria("attributes.op", "and").build(),
                job -> assertThat(job.getJobDescriptor().getAttributesMap())
                        .containsKey("attr1")
                        .containsEntry("attr2", "value2")
        );
        observeByJobDescriptor(
                jobsScenarioBuilder.takeJobId(4),
                ObserveJobsQuery.newBuilder().putFilteringCriteria("jobType", "service").build(),
                job -> assertThat(job.getJobDescriptor().getJobSpecCase()).isEqualTo(SERVICE)
        );
    }

    private void observeByJobDescriptor(String jobId, ObserveJobsQuery query, Consumer<Job> check) {
        Iterator<JobChangeNotification> eventIt = client.observeJobs(query);
        while (eventIt.hasNext()) {
            JobChangeNotification event = eventIt.next();
            if (event.hasJobUpdate()) {
                Job job = event.getJobUpdate().getJob();
                assertThat(job.getId()).isEqualTo(jobId);
                check.accept(job);
                return;
            }
        }
        fail(String.format("Expected job event not found: jobId=%s, query=%s", jobId, query));
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeByStates() {
        Iterator<JobChangeNotification> eventsWithJobStateFilter = client.observeJobs(
                ObserveJobsQuery.newBuilder().putFilteringCriteria("jobState", JobState.KillInitiated.toString()).build()
        );
        Iterator<JobChangeNotification> eventsWithTaskStateFilter = client.observeJobs(
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("taskStates", String.join(",", Arrays.asList(
                                TaskState.Launched.toString(),
                                TaskState.Started.toString()
                        )))
                        .build()
        );
        assertNextIsSnapshot(eventsWithJobStateFilter);
        assertNextIsSnapshot(eventsWithTaskStateFilter);

        String jobId = jobsScenarioBuilder.scheduleAndReturnJob(batchJobDescriptors().getValue(), jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .template(killJob())
        ).getId();

        assertNextIsJobEvent(eventsWithJobStateFilter, job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated));
        assertNextIsTaskEvent(eventsWithJobStateFilter, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated));

        assertNextIsTaskEvent(eventsWithTaskStateFilter, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Launched));
        assertNextIsTaskEvent(eventsWithTaskStateFilter, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Started));
        assertNextIsJobEvent(eventsWithTaskStateFilter, job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated));
    }

    @SafeVarargs
    private final <E extends JobDescriptor.JobDescriptorExt> void startAll(JobDescriptor<E>... descriptors) {
        for (JobDescriptor<E> descriptor : descriptors) {
            jobsScenarioBuilder.schedule(descriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startTasksInNewJob()));
        }
    }

    private void assertNextIsSnapshot(Iterator<JobChangeNotification> eventsIt) {
        assertThat(eventsIt.hasNext()).isTrue();
        assertThat(eventsIt.next().hasSnapshotEnd()).isTrue();
    }

    private void assertNextIsJobEvent(Iterator<JobChangeNotification> eventIt, Consumer<Job> check) {
        assertThat(eventIt.hasNext()).isTrue();
        JobChangeNotification event = eventIt.next();
        assertThat(event.hasJobUpdate()).isTrue();
        check.accept(event.getJobUpdate().getJob());
    }

    private void assertNextIsTaskEvent(Iterator<JobChangeNotification> eventIt, Consumer<Task> check) {
        assertThat(eventIt.hasNext()).isTrue();
        JobChangeNotification event = eventIt.next();
        assertThat(event.hasTaskUpdate()).isTrue();
        check.accept(event.getTaskUpdate().getTask());
    }
}
