/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.job.other;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.embedded.federation.EmbeddedTitusFederation;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicKubeCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobFederationTest extends BaseIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(JobFederationTest.class);

    private final String federatedStackName = UUID.randomUUID().toString();

    @Rule
    public final TitusStackResource titusStackResource = new TitusStackResource(
            EmbeddedTitusFederation.aDefaultTitusFederation()
                    .withProperty("titus.federation.stack", federatedStackName)
                    .withCell("a.*", basicKubeCell("defaultCell", 2))
                    .withCell("b.*", basicKubeCell("v3OnlyCell", 2))
                    .build()
    );
    private JobManagementServiceGrpc.JobManagementServiceBlockingStub blockingJobClient;
    private SchedulerServiceGrpc.SchedulerServiceBlockingStub blockingSchedulerClient;

    private TestStreamObserver<JobChangeNotification> eventStreamObserver;

    @Before
    public void setUp() throws Exception {
        this.blockingJobClient = titusStackResource.getOperations().getV3BlockingGrpcClient();
        this.blockingSchedulerClient = titusStackResource.getOperations().getV3BlockingSchedulerClient();
        this.eventStreamObserver = new TestStreamObserver<>();

        JobManagementServiceGrpc.JobManagementServiceStub asyncClient = titusStackResource.getOperations().getV3GrpcClient();
        asyncClient.observeJobs(ObserveJobsQuery.newBuilder().build(), eventStreamObserver);
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void testJobCreateRouting() {
        Map<String, Job> jobs = new ConcurrentHashMap<>();
        Map<String, Task> tasks = new ConcurrentHashMap<>();
        eventStreamObserver.toObservable().subscribe(
                event -> {
                    switch (event.getNotificationCase()) {
                        case JOBUPDATE:
                            Job job = event.getJobUpdate().getJob();
                            jobs.put(job.getId(), job);
                            break;
                        case TASKUPDATE:
                            Task task = event.getTaskUpdate().getTask();
                            tasks.put(task.getJobId(), task);
                            break;
                    }
                }
        );

        String cell1JobId = blockingJobClient.createJob(toGrpcJobDescriptor(oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("a123").build())).getId();
        String cell2JobId = blockingJobClient.createJob(toGrpcJobDescriptor(oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("b123").build())).getId();


        await().timeout(5, TimeUnit.SECONDS).until(() -> tasks.containsKey(cell1JobId));
        await().timeout(5, TimeUnit.SECONDS).until(() -> tasks.containsKey(cell2JobId));

        Map<String, String> cell1JobAttributes = jobs.get(cell1JobId).getJobDescriptor().getAttributesMap();
        assertThat(cell1JobAttributes).containsEntry("titus.stack", federatedStackName);
        assertThat(cell1JobAttributes).containsEntry("titus.cell", "defaultCell");
        Map<String, String> cell2JobAttributes = jobs.get(cell2JobId).getJobDescriptor().getAttributesMap();
        assertThat(cell2JobAttributes).containsEntry("titus.stack", federatedStackName);
        assertThat(cell2JobAttributes).containsEntry("titus.cell", "v3OnlyCell");

        Map<String, String> cell1TaskContext = tasks.get(cell1JobId).getTaskContextMap();
        assertThat(cell1TaskContext).containsEntry("titus.stack", federatedStackName);
        assertThat(cell1TaskContext).containsEntry("titus.cell", "defaultCell");
        Map<String, String> cell2TaskContext = tasks.get(cell2JobId).getTaskContextMap();
        assertThat(cell2TaskContext).containsEntry("titus.stack", federatedStackName);
        assertThat(cell2TaskContext).containsEntry("titus.cell", "v3OnlyCell");
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void testLastSchedulingResult() {
        // Ask for a lot of
        JobDescriptor<BatchJobExt> tooBigJob = oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().toBuilder()
                .withContainerResources(ContainerResources.newBuilder()
                        .withCpu(64)
                        .withGpu(0)
                        .withMemoryMB(472_000)
                        .withDiskMB(999_000)
                        .withNetworkMbps(40_000)
                        .build()
                )
                .build()
        );
        blockingJobClient.createJob(toGrpcJobDescriptor(tooBigJob.toBuilder().withCapacityGroup("a123").build())).getId();
        blockingJobClient.createJob(toGrpcJobDescriptor(tooBigJob.toBuilder().withCapacityGroup("b123").build())).getId();

        List<Task> tasks = eventStreamObserver.toObservable()
                .filter(event -> event.getNotificationCase() == JobChangeNotification.NotificationCase.TASKUPDATE)
                .map(e -> e.getTaskUpdate().getTask())
                .take(2)
                .toList()
                .timeout(5, TimeUnit.SECONDS)
                .toBlocking()
                .first();

        checkSchedulingResult(tasks.get(0));
        checkSchedulingResult(tasks.get(1));
    }

    private void checkSchedulingResult(Task task) {
        await().timeout(30, TimeUnit.SECONDS).until(() -> {
            try {
                SchedulingResultEvent result = blockingSchedulerClient.getSchedulingResult(SchedulingResultRequest.newBuilder()
                        .setTaskId(task.getId())
                        .build()
                );
                logger.info("Received scheduling result: {}", result);

                if (result.getFailures().getFailuresList().size() >= 1) {
                    return true;
                }
            } catch (Exception e) {
                logger.warn("Scheduling result query error", e);
            }
            return false;
        });
    }
}
