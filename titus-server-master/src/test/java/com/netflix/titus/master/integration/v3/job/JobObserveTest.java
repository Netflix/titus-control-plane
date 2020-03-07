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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
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
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import rx.observers.AssertableSubscriber;

import static com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase.SERVICE;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.killJob;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
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

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeJobs() throws Exception {
        TestStreamObserver<JobChangeNotification> eventObserver = observe(ObserveJobsQuery.newBuilder().build());

        CountDownLatch latch = new CountDownLatch(2);
        AssertableSubscriber<JobChangeNotification> events = eventObserver.toObservable().doOnNext(n -> {
            if (n.getNotificationCase() == NotificationCase.JOBUPDATE &&
                    n.getJobUpdate().getJob().getStatus().getState() == JobState.Finished) {
                latch.countDown();
            }
        }).test();

        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .killJob()
        );
        jobsScenarioBuilder.schedule(oneTaskServiceJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .killJob()
        );

        if (!latch.await(25, TimeUnit.SECONDS)) {
            fail("jobs did not finish within 25s");
        }

        assertThat(events.getValueCount()).isGreaterThan(0);
        events.getOnNextEvents().stream()
                .filter(n -> n.getNotificationCase() == NotificationCase.JOBUPDATE)
                .forEach(n -> CellAssertions.assertCellInfo(n.getJobUpdate().getJob(), EmbeddedTitusMaster.CELL_NAME));
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeSnapshotWithFilter() throws Exception {
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
        ObserveJobsQuery query = ObserveJobsQuery.newBuilder()
                .putFilteringCriteria("applicationName", "myApp")
                .build();
        TestStreamObserver<JobChangeNotification> eventObserver = observe(query);

        CountDownLatch latch = new CountDownLatch(1);
        AssertableSubscriber<JobChangeNotification> events = eventObserver.toObservable().doOnNext(n -> {
            if (n.getNotificationCase() == NotificationCase.SNAPSHOTEND) {
                latch.countDown();
            }
        }).test();
        if (!latch.await(25, TimeUnit.SECONDS)) {
            fail("snapshot did not finish within 25s");
        }

        assertEvents(eventObserver,
                job -> assertThat(job.getJobDescriptor().getApplicationName()).isEqualTo("myApp"),
                task -> assertThat(task.getJobId()).isEqualTo(myAppJobId)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeByJobDescriptor() throws Exception {
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

        List<TestStreamObserver<JobChangeNotification>> observers = observeAll(
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("applicationName", "myApp")
                        .build(),
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("owner", "me@netflix.com")
                        .build(),
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("imageName", "some/image")
                        .putFilteringCriteria("imageTag", "stable")
                        .build(),
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("attributes", "attr1,attr2:value2")
                        .putFilteringCriteria("attributes.op", "and")
                        .build(),
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("jobType", "service")
                        .build()
        );

        CountDownLatch latch = new CountDownLatch(observers.size());
        for (TestStreamObserver<JobChangeNotification> eventObserver : observers) {
            eventObserver.toObservable().doOnNext(n -> {
                if (n.getNotificationCase() == NotificationCase.TASKUPDATE &&
                        n.getTaskUpdate().getTask().getStatus().getState() == TaskState.Started) {
                    latch.countDown();
                }
            }).test();
        }

        String myAppJobId = jobsScenarioBuilder.takeJobId(0);
        String meOwnerJobId = jobsScenarioBuilder.takeJobId(1);
        String someImageJobId = jobsScenarioBuilder.takeJobId(2);
        String attributesJobId = jobsScenarioBuilder.takeJobId(3);
        String serviceJobId = jobsScenarioBuilder.takeJobId(4);

        if (!latch.await(25, TimeUnit.SECONDS)) {
            fail("all tasks did not start within 25s");
        }

        assertEvents(observers.get(0),
                job -> assertThat(job.getJobDescriptor().getApplicationName()).isEqualTo("myApp"),
                task -> assertThat(task.getJobId()).isEqualTo(myAppJobId)
        );
        assertEvents(observers.get(1),
                job -> assertThat(job.getJobDescriptor().getOwner().getTeamEmail()).isEqualTo("me@netflix.com"),
                task -> assertThat(task.getJobId()).isEqualTo(meOwnerJobId)
        );
        assertEvents(observers.get(2),
                job -> {
                    Image image = job.getJobDescriptor().getContainer().getImage();
                    assertThat(image.getName()).isEqualTo("some/image");
                    assertThat(image.getTag()).isEqualTo("stable");
                },
                task -> assertThat(task.getJobId()).isEqualTo(someImageJobId)
        );
        assertEvents(observers.get(3),
                job -> assertThat(job.getJobDescriptor().getAttributesMap())
                        .containsKey("attr1")
                        .containsEntry("attr2", "value2"),
                task -> assertThat(task.getJobId()).isEqualTo(attributesJobId)
        );
        assertEvents(observers.get(4),
                job -> {
                    assertThat(job.getJobDescriptor().getJobSpecCase()).isEqualTo(SERVICE);
                    assertThat(job.getId()).isEqualTo(serviceJobId);
                },
                task -> assertThat(task.getJobId()).isEqualTo(serviceJobId)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void observeByStates() throws Exception {
        List<TestStreamObserver<JobChangeNotification>> observers = observeAll(
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("jobState", JobState.KillInitiated.toString())
                        .build(),
                ObserveJobsQuery.newBuilder()
                        .putFilteringCriteria("taskStates", String.join(",", Arrays.asList(
                                TaskState.Launched.toString(),
                                TaskState.Started.toString()
                        )))
                        .build()
        );
        CountDownLatch latch = new CountDownLatch(2);
        // look for task events since the job ones are being filtered
        observers.get(0).toObservable().doOnNext(n -> {
            if (n.getNotificationCase() == NotificationCase.TASKUPDATE &&
                    n.getTaskUpdate().getTask().getStatus().getState() == TaskState.Finished) {
                latch.countDown();
            }
        }).test();
        // look for job events since the task ones are being filtered
        observers.get(1).toObservable().doOnNext(n -> {
            if (n.getNotificationCase() == NotificationCase.JOBUPDATE &&
                    n.getJobUpdate().getJob().getStatus().getState() == JobState.KillInitiated) {
                latch.countDown();
            }
        }).test();

        jobsScenarioBuilder.schedule(batchJobDescriptors().getValue(), jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .template(killJob())
        );
        String jobId = jobsScenarioBuilder.takeJobId(0);

        if (!latch.await(25, TimeUnit.SECONDS)) {
            fail("timed out waiting streams to see finished events");
        }

        assertEvents(observers.get(0),
                job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated),
                task -> assertThat(task.getJobId()).isEqualTo(jobId)
        );
        assertEvents(observers.get(1),
                job -> assertThat(job.getId()).isEqualTo(jobId),
                task -> assertThat(task.getStatus().getState()).isIn(TaskState.Launched, TaskState.Started)
        );
    }

    @SafeVarargs
    private final <E extends JobDescriptor.JobDescriptorExt> void startAll(JobDescriptor<E>... descriptors) throws Exception {
        for (JobDescriptor<E> descriptor : descriptors) {
            jobsScenarioBuilder.schedule(descriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startTasksInNewJob()));
        }
    }

    private TestStreamObserver<JobChangeNotification> observe(ObserveJobsQuery query) {
        TestStreamObserver<JobChangeNotification> eventObserver = new TestStreamObserver<>();
        titusStackResource.getGateway().getV3GrpcClient()
                .withDeadlineAfter(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .observeJobs(query, eventObserver);
        return eventObserver;
    }

    private List<TestStreamObserver<JobChangeNotification>> observeAll(ObserveJobsQuery... queries) {
        return Stream.of(queries).map(this::observe).collect(Collectors.toList());
    }

    private void assertEvents(TestStreamObserver<JobChangeNotification> events, Consumer<Job> jobAssertions, Consumer<Task> taskAssertions) {
        List<JobChangeNotification> emittedItems = events.getEmittedItems();
        assertThat(emittedItems).isNotEmpty();
        for (JobChangeNotification notification : emittedItems) {
            switch (notification.getNotificationCase()) {
                case JOBUPDATE:
                    jobAssertions.accept(notification.getJobUpdate().getJob());
                    break;
                case TASKUPDATE:
                    taskAssertions.accept(notification.getTaskUpdate().getTask());
                    break;
            }
        }
        events.cancel();
    }
}
