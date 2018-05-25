package com.netflix.titus.master.integration.v3.job;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
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

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicV3OnlyCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobFederationTest extends BaseIntegrationTest {
    private final String federatedStackName = UUID.randomUUID().toString();

    @Rule
    public final TitusStackResource titusStackResource = new TitusStackResource(
            EmbeddedTitusFederation.aDefaultTitusFederation()
                    .withProperty("titus.federation.stack", federatedStackName)
                    .withCell("a.*", basicCell("defaultCell", 2))
                    .withCell("b.*", basicV3OnlyCell("v3OnlyCell", 2))
                    .build()
    );
    private JobManagementServiceGrpc.JobManagementServiceBlockingStub blockingClient;
    private TestStreamObserver<JobChangeNotification> eventStreamObserver;

    @Before
    public void setUp() throws Exception {
        this.blockingClient = titusStackResource.getOperations().getV3BlockingGrpcClient();
        this.eventStreamObserver = new TestStreamObserver<>();

        JobManagementServiceGrpc.JobManagementServiceStub asyncClient = titusStackResource.getOperations().getV3GrpcClient();
        asyncClient.observeJobs(Empty.getDefaultInstance(), eventStreamObserver);
    }

    @Test
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

        String cell1JobId = blockingClient.createJob(toGrpcJobDescriptor(oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("a123").build())).getId();
        String cell2JobId = blockingClient.createJob(toGrpcJobDescriptor(oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("b123").build())).getId();


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
}
