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

package com.netflix.titus.runtime.endpoint.v3.rest;

import java.util.Optional;

import com.google.protobuf.UInt32Value;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudget;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil;
import com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcDisruptionBudget;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJob;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcTask;
import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.JUNIT_REST_CALL_METADATA;
import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.NEXT_PAGE_OF_2;
import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.paginationOf;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebMvcTest()
@ContextConfiguration(classes = {JobManagementSpringResource.class, ProtobufHttpMessageConverter.class})
public class JobManagementSpringResourceTest {

    private static final Job JOB_1 = toGrpcJob(JobGenerator.oneBatchJob()).toBuilder().setId("myJob1").build();
    private static final Job JOB_2 = toGrpcJob(JobGenerator.oneBatchJob()).toBuilder().setId("myJob2").build();

    private static final String JOB_ID_1 = JOB_1.getId();

    private static final JobDescriptor JOB_DESCRIPTOR_1 = toGrpcJobDescriptor(JobDescriptorGenerator.oneTaskBatchJobDescriptor());

    private static final Task TASK_1 = toGrpcTask(JobGenerator.oneBatchTask(), EmptyLogStorageInfo.empty()).toBuilder().setId("myTask1").build();
    private static final Task TASK_2 = toGrpcTask(JobGenerator.oneBatchTask(), EmptyLogStorageInfo.empty()).toBuilder().setId("myTask2").build();

    private static final String TASK_ID_1 = TASK_1.getId();

    @MockBean
    public JobServiceGateway jobServiceGatewayMock;

    @MockBean
    public SystemLogService systemLog;

    @Autowired
    private MockMvc mockMvc;

    @Before
    public void setUp() {
    }

    @Test
    public void testCreateJob() throws Exception {
        when(jobServiceGatewayMock.createJob(JOB_DESCRIPTOR_1, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(JOB_ID_1));
        JobId entity = SpringMockMvcUtil.doPost(mockMvc, "/api/v3/jobs", JOB_DESCRIPTOR_1, JobId.class);
        assertThat(entity.getId()).isEqualTo(JOB_ID_1);

        verify(jobServiceGatewayMock, times(1)).createJob(JOB_DESCRIPTOR_1, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testSetInstances() throws Exception {
        Capacity capacity = Capacity.newBuilder().setMin(1).setDesired(2).setMax(3).build();
        JobCapacityUpdate forwardedRequest = JobCapacityUpdate.newBuilder().setJobId(JOB_ID_1).setCapacity(capacity).build();

        when(jobServiceGatewayMock.updateJobCapacity(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPut(mockMvc, String.format("/api/v3/jobs/%s/instances", JOB_ID_1), capacity);

        verify(jobServiceGatewayMock, times(1)).updateJobCapacity(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testSetCapacityWithOptionalAttributes() throws Exception {
        JobCapacityWithOptionalAttributes restRequest = JobCapacityWithOptionalAttributes.newBuilder()
                .setMin(UInt32Value.newBuilder().setValue(1).build())
                .setDesired(UInt32Value.newBuilder().setValue(2).build())
                .setMax(UInt32Value.newBuilder().setValue(3).build())
                .build();
        JobCapacityUpdateWithOptionalAttributes forwardedRequest = JobCapacityUpdateWithOptionalAttributes.newBuilder()
                .setJobId(JOB_ID_1)
                .setJobCapacityWithOptionalAttributes(restRequest)
                .build();

        when(jobServiceGatewayMock.updateJobCapacityWithOptionalAttributes(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPut(mockMvc, String.format("/api/v3/jobs/%s/capacityAttributes", JOB_ID_1), restRequest);

        verify(jobServiceGatewayMock, times(1)).updateJobCapacityWithOptionalAttributes(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testSetJobProcesses() throws Exception {
        ServiceJobSpec.ServiceJobProcesses restRequest = ServiceJobSpec.ServiceJobProcesses.newBuilder()
                .setDisableDecreaseDesired(true)
                .setDisableIncreaseDesired(true)
                .build();

        JobProcessesUpdate forwardedRequest = JobProcessesUpdate.newBuilder()
                .setJobId(JOB_ID_1)
                .setServiceJobProcesses(restRequest)
                .build();

        when(jobServiceGatewayMock.updateJobProcesses(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPut(mockMvc, String.format("/api/v3/jobs/%s/jobprocesses", JOB_ID_1), restRequest);

        verify(jobServiceGatewayMock, times(1)).updateJobProcesses(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testSetJobDisruptionBudget() throws Exception {
        JobDisruptionBudget restRequest = toGrpcDisruptionBudget(DisruptionBudgetGenerator.budgetSelfManagedBasic());

        JobDisruptionBudgetUpdate forwardedRequest = JobDisruptionBudgetUpdate.newBuilder()
                .setJobId(JOB_ID_1)
                .setDisruptionBudget(restRequest)
                .build();

        when(jobServiceGatewayMock.updateJobDisruptionBudget(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Mono.empty());
        SpringMockMvcUtil.doPut(mockMvc, String.format("/api/v3/jobs/%s/disruptionBudget", JOB_ID_1), restRequest);

        verify(jobServiceGatewayMock, times(1)).updateJobDisruptionBudget(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testUpdateJobAttributes() throws Exception {
        JobAttributesUpdate restRequest = JobAttributesUpdate.newBuilder()
                .setJobId(JOB_ID_1)
                .putAttributes("keyA", "valueA")
                .build();

        when(jobServiceGatewayMock.updateJobAttributes(restRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Mono.empty());
        SpringMockMvcUtil.doPut(mockMvc, String.format("/api/v3/jobs/%s/attributes", JOB_ID_1), restRequest);

        verify(jobServiceGatewayMock, times(1)).updateJobAttributes(restRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testDeleteJobAttributes() throws Exception {
        JobAttributesDeleteRequest forwardedRequest = JobAttributesDeleteRequest.newBuilder()
                .setJobId(JOB_ID_1)
                .addKeys("keyA")
                .build();
        when(jobServiceGatewayMock.deleteJobAttributes(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Mono.empty());
        SpringMockMvcUtil.doDelete(mockMvc, String.format("/api/v3/jobs/%s/attributes", JOB_ID_1), "keys", "keyA");

        verify(jobServiceGatewayMock, times(1)).deleteJobAttributes(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testEnableJob() throws Exception {
        JobStatusUpdate forwardedRequest = JobStatusUpdate.newBuilder()
                .setId(JOB_ID_1)
                .setEnableStatus(true)
                .build();
        when(jobServiceGatewayMock.updateJobStatus(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPost(mockMvc, String.format("/api/v3/jobs/%s/enable", JOB_ID_1));

        verify(jobServiceGatewayMock, times(1)).updateJobStatus(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testDisableJob() throws Exception {
        JobStatusUpdate forwardedRequest = JobStatusUpdate.newBuilder()
                .setId(JOB_ID_1)
                .setEnableStatus(false)
                .build();
        when(jobServiceGatewayMock.updateJobStatus(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPost(mockMvc, String.format("/api/v3/jobs/%s/disable", JOB_ID_1));

        verify(jobServiceGatewayMock, times(1)).updateJobStatus(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testFindJob() throws Exception {
        when(jobServiceGatewayMock.findJob(JOB_ID_1, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(JOB_1));
        Job entity = SpringMockMvcUtil.doGet(mockMvc, String.format("/api/v3/jobs/%s", JOB_ID_1), Job.class);
        assertThat(entity).isEqualTo(JOB_1);

        verify(jobServiceGatewayMock, times(1)).findJob(JOB_ID_1, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testFindJobs() throws Exception {
        JobQuery forwardedRequest = JobQuery.newBuilder()
                .putFilteringCriteria("filter1", "value1")
                .setPage(NEXT_PAGE_OF_2)
                .build();
        JobQueryResult expectedResult = JobQueryResult.newBuilder()
                .setPagination(paginationOf(NEXT_PAGE_OF_2))
                .addAllItems(asList(JOB_1, JOB_2))
                .build();
        when(jobServiceGatewayMock.findJobs(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(expectedResult));
        JobQueryResult entity = SpringMockMvcUtil.doPaginatedGet(mockMvc, "/api/v3/jobs", JobQueryResult.class, NEXT_PAGE_OF_2, "filter1", "value1");
        assertThat(entity).isEqualTo(expectedResult);

        verify(jobServiceGatewayMock, times(1)).findJobs(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testKillJob() throws Exception {
        when(jobServiceGatewayMock.killJob(JOB_ID_1, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doDelete(mockMvc, String.format("/api/v3/jobs/%s", JOB_ID_1));

        verify(jobServiceGatewayMock, times(1)).killJob(JOB_ID_1, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testFindTask() throws Exception {
        when(jobServiceGatewayMock.findTask(TASK_ID_1, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(TASK_1));
        Task entity = SpringMockMvcUtil.doGet(mockMvc, String.format("/api/v3/tasks/%s", TASK_ID_1), Task.class);
        assertThat(entity).isEqualTo(TASK_1);

        verify(jobServiceGatewayMock, times(1)).findTask(TASK_ID_1, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testFindTasks() throws Exception {
        TaskQuery forwardedRequest = TaskQuery.newBuilder()
                .putFilteringCriteria("filter1", "value1")
                .setPage(NEXT_PAGE_OF_2)
                .build();
        TaskQueryResult expectedResult = TaskQueryResult.newBuilder()
                .setPagination(paginationOf(NEXT_PAGE_OF_2))
                .addAllItems(asList(TASK_1, TASK_2))
                .build();
        when(jobServiceGatewayMock.findTasks(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(expectedResult));
        TaskQueryResult entity = SpringMockMvcUtil.doPaginatedGet(mockMvc, "/api/v3/tasks", TaskQueryResult.class, NEXT_PAGE_OF_2, "filter1", "value1");
        assertThat(entity).isEqualTo(expectedResult);

        verify(jobServiceGatewayMock, times(1)).findTasks(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void taskKillTask() throws Exception {
        TaskKillRequest forwardedRequest = TaskKillRequest.newBuilder()
                .setTaskId(TASK_ID_1)
                .setShrink(true)
                .setPreventMinSizeUpdate(true)
                .build();
        when(jobServiceGatewayMock.killTask(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doDelete(mockMvc, String.format("/api/v3/tasks/%s", TASK_ID_1), "shrink", "true", "preventMinSizeUpdate", "true");

        verify(jobServiceGatewayMock, times(1)).killTask(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testUpdateTaskAttributes() throws Exception {
        TaskAttributesUpdate restRequest = TaskAttributesUpdate.newBuilder()
                .setTaskId(TASK_ID_1)
                .putAttributes("keyA", "valueA")
                .build();

        when(jobServiceGatewayMock.updateTaskAttributes(restRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPut(mockMvc, String.format("/api/v3/tasks/%s/attributes", TASK_ID_1), restRequest);

        verify(jobServiceGatewayMock, times(1)).updateTaskAttributes(restRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testDeleteTaskAttributes() throws Exception {
        TaskAttributesDeleteRequest forwardedRequest = TaskAttributesDeleteRequest.newBuilder()
                .setTaskId(TASK_ID_1)
                .addKeys("keyA")
                .build();
        when(jobServiceGatewayMock.deleteTaskAttributes(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doDelete(mockMvc, String.format("/api/v3/tasks/%s/attributes", TASK_ID_1), "keys", "keyA");

        verify(jobServiceGatewayMock, times(1)).deleteTaskAttributes(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testMoveTask() throws Exception {
        TaskMoveRequest request = TaskMoveRequest.newBuilder()
                .setSourceJobId(JOB_ID_1)
                .setTargetJobId(JOB_2.getId())
                .setTaskId(TASK_ID_1)
                .build();
        when(jobServiceGatewayMock.moveTask(request, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPost(mockMvc, "/api/v3/tasks/move", request);

        verify(jobServiceGatewayMock, times(1)).moveTask(request, JUNIT_REST_CALL_METADATA);
    }
}
