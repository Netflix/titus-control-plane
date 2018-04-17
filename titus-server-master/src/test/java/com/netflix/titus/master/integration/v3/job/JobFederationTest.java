package com.netflix.titus.master.integration.v3.job;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;
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

    @Rule
    public final TitusStackResource titusStackResource = new TitusStackResource(
            EmbeddedTitusFederation.aDefaultTitusFederation()
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
        Map<String, Task> tasks = new ConcurrentHashMap<>();
        eventStreamObserver.toObservable().subscribe(
                event -> {
                    if (event.getNotificationCase() == JobChangeNotification.NotificationCase.TASKUPDATE) {
                        Task task = event.getTaskUpdate().getTask();
                        tasks.put(task.getJobId(), task);
                    }
                }
        );

        String cell1JobId = blockingClient.createJob(toGrpcJobDescriptor(oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("a123").build())).getId();
        String cell2JobId = blockingClient.createJob(toGrpcJobDescriptor(oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("b123").build())).getId();


        await().timeout(5, TimeUnit.SECONDS).until(() -> tasks.containsKey(cell1JobId));
        await().timeout(5, TimeUnit.SECONDS).until(() -> tasks.containsKey(cell2JobId));

        assertThat(tasks.get(cell1JobId).getTaskContextMap().get("titus.cell")).isEqualTo("defaultCell");
        assertThat(tasks.get(cell2JobId).getTaskContextMap().get("titus.cell")).isEqualTo("v3OnlyCell");
    }
}
