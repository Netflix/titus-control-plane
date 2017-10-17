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

package io.netflix.titus.master.endpoint.v2.rest;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import io.netflix.titus.api.endpoint.v2.rest.representation.JobSubmitReply;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TypeReferences;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.ConfigurationMockSamples;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.endpoint.v2.V2LegacyTitusServiceGateway;
import io.netflix.titus.master.endpoint.v2.rest.caller.NoOpCallerIdResolver;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobKillCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInServiceCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TaskKillCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.endpoint.v2.validator.ValidatorConfiguration;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.runtime.endpoint.common.rest.ErrorResponse;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import io.netflix.titus.testkit.junit.jaxrs.HttpTestClient;
import io.netflix.titus.testkit.junit.jaxrs.HttpTestClientException;
import io.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobManagementResourceTest {

    private static final V2LegacyTitusServiceGateway serviceGateway = mock(V2LegacyTitusServiceGateway.class);

    private static final MasterConfiguration configuration = ConfigurationMockSamples.withJobSpec(mock(MasterConfiguration.class));

    private static final ValidatorConfiguration validatorConfiguration = mock(ValidatorConfiguration.class);

    private static RxEventBus eventBus = mock(RxEventBus.class);

    private static final JobManagementResource restService = new JobManagementResource(
            serviceGateway, configuration, validatorConfiguration, null, NoOpCallerIdResolver.INSTANCE, eventBus);

    @ClassRule
    public static final JaxRsServerResource<JobManagementResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static HttpTestClient client;

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    @BeforeClass
    public static void setUpClass() throws Exception {
        client = new HttpTestClient(jaxRsServer.getBaseURI());
    }

    @Before
    public void setUp() throws Exception {
        Mockito.reset(serviceGateway);
    }

    @Test
    public void getAllJobs() throws Exception {
        JobQueryCriteria<TitusTaskState, TitusJobType> criteria = JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder()
                .withTaskStates(TitusTaskState.ANY)
                .withIncludeArchived(true)
                .build();
        getAllJobs(criteria, singletonMap("taskState", "ANY"));
    }

    @Test
    public void getAllJobsWithTaskStatusFilter() throws Exception {
        JobQueryCriteria<TitusTaskState, TitusJobType> criteria = JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder()
                .withTaskStates(Collections.singleton(TitusTaskState.RUNNING))
                .build();
        getAllJobs(criteria, singletonMap("taskState", "RUNNING"));
    }

    @Test
    public void getAllJobsWithLimitFilter() throws Exception {
        JobQueryCriteria<TitusTaskState, TitusJobType> criteria = JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder()
                .withLimit(10)
                .build();
        getAllJobs(criteria, singletonMap("limit", "10"));
    }

    @Test
    public void getAllJobsWithLabelFilter() throws Exception {
        JobQueryCriteria<TitusTaskState, TitusJobType> criteria = JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder()
                .withLabels(singletonMap("labelA", asSet("valueA")))
                .build();
        getAllJobs(criteria, singletonMap("labels", "labelA=valueA"));
    }

    @Test
    public void getAllJobsWithLabelJobGroupFilter() throws Exception {
        JobQueryCriteria<TitusTaskState, TitusJobType> criteria = JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder()
                .withJobGroupStack("myStack")
                .withJobGroupDetail("myDetail")
                .withJobGroupSequence("v001")
                .build();
        getAllJobs(criteria,
                ImmutableMap.<String, Object>builder()
                        .put("jobGroupStack", "myStack")
                        .put("jobGroupDetail", "myDetail")
                        .put("jobGroupSequence", "v001")
                        .build()
        );
    }

    private void getAllJobs(JobQueryCriteria<TitusTaskState, TitusJobType> criteria, Map<String, Object> queryParams) throws java.io.IOException {
        TitusJobSpec jobSpec = generator.newJobSpec(TitusJobType.batch, "myJob");
        TitusJobInfo jobInfo = generator.newJobInfo(jobSpec);
        when(serviceGateway.findJobsByCriteria(criteria, Optional.empty())).thenReturn(Pair.of(singletonList(jobInfo), null));

        List<TitusJobInfo> titusJobSpec = client.doGET("/api/v2/jobs", queryParams, TypeReferences.TITUS_JOB_INFO_LIST_TREF);

        verify(serviceGateway, times(1)).findJobsByCriteria(criteria, Optional.empty());
        assertThat(titusJobSpec).hasSize(1);
    }

    @Test
    public void getAllJobsWithNoFilterIsNotAllowed() throws Exception {
        try {
            client.doGET("/api/v2/jobs", TypeReferences.TITUS_JOB_INFO_LIST_TREF);
            fail("Expected to fail, as no query parameters are provided");
        } catch (HttpTestClientException e) {
            assertThat(e.getStatusCode()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        }
    }

    @Test
    public void getJobById() throws Exception {
        TitusJobSpec jobSpec = generator.newJobSpec(TitusJobType.batch, "myJob");
        TitusJobInfo jobInfo = generator.newJobInfo(jobSpec);
        when(serviceGateway.findJobById(any(), anyBoolean(), any())).thenReturn(Observable.just(jobInfo));

        TitusJobInfo titusJobInfo = client.doGET("/api/v2/jobs/" + jobInfo.getId(), singletonMap("taskState", "FAILED"), TitusJobInfo.class);
        verify(serviceGateway, times(1)).findJobById(jobInfo.getId(), true, asSet(TitusTaskState.FAILED));
        assertThat(titusJobInfo.getId()).isEqualTo(jobInfo.getId());
    }

    @Test
    public void submitJob() throws Exception {
        TitusJobSpec jobSpec = generator.newJobSpec(TitusJobType.batch, "myJob");
        when(serviceGateway.createJob(any())).thenReturn(Observable.just("myJobId"));

        String jobLocation = "/api/v2/jobs/myJobId";
        JobSubmitReply reply = client.doPOST("/api/v2/jobs", jobSpec, jobLocation, JobSubmitReply.class);
        assertThat(reply.getJobUri()).isEqualTo(jobLocation);
    }

    @Test
    public void submitJobService() throws Exception {
        TitusJobSpec jobSpec = generator.newJobSpec(TitusJobType.service, "myServiceJob");
        when(serviceGateway.createJob(any())).thenReturn(Observable.just("myServiceJobId"));

        String jobLocation = "/api/v2/jobs/myServiceJobId";
        JobSubmitReply reply = client.doPOST("/api/v2/jobs", jobSpec, jobLocation, JobSubmitReply.class);
        assertThat(reply.getJobUri()).isEqualTo(jobLocation);
    }

    @Test
    public void setInstanceCount() throws Exception {
        when(serviceGateway.resizeJob(any(), any(), anyInt(), anyInt(), anyInt())).thenReturn(Observable.empty());
        client.doPOST("/api/v2/jobs/setinstancecounts", new JobSetInstanceCountsCmd("myUser", "myJobId", 3, 1, 5), null);

        verify(serviceGateway, times(1)).resizeJob("myUser", "myJobId", 3, 1, 5);
    }

    @Test
    public void setInServiceStatus() throws Exception {
        TitusJobSpec jobSpec = generator.newJobSpec(TitusJobType.batch, "myJob");
        TitusJobInfo jobInfo = generator.newJobInfo(jobSpec);
        when(serviceGateway.findJobsByCriteria(any(), any())).thenReturn(Pair.of(singletonList(jobInfo), null));

        when(serviceGateway.changeJobInServiceStatus(any(), any(), anyBoolean())).thenReturn(Observable.empty());
        client.doPOST("/api/v2/jobs/setinservice", new JobSetInServiceCmd("myUser", jobInfo.getId(), false), null);

        verify(serviceGateway, times(1)).changeJobInServiceStatus("myUser", jobInfo.getId(), false);
    }

    @Test
    public void killJobCmd() throws Exception {
        when(serviceGateway.killJob(any(), any())).thenReturn(Observable.empty());
        client.doPOST("/api/v2/jobs/kill", new JobKillCmd("myUser", "myJobId"), null);

        verify(serviceGateway, times(1)).killJob("myUser", "myJobId");
    }

    @Test
    public void getTaskById() throws Exception {
        String jobId = generator.newJobInfo(TitusJobType.batch, "myJob").getId();
        generator.scheduleJob(jobId);
        TitusTaskInfo titusTaskInfo = generator.getTitusTaskInfos(jobId).get(0);

        when(serviceGateway.findTaskById(any())).thenReturn(Observable.just(titusTaskInfo));
        TitusTaskInfo reply = client.doGET("/api/v2/tasks/" + titusTaskInfo.getId(), TitusTaskInfo.class);

        verify(serviceGateway, times(1)).findTaskById(titusTaskInfo.getId());
        assertThat(reply.getId()).isEqualTo(titusTaskInfo.getId());
    }

    @Test
    public void killSingleTask() throws Exception {
        when(serviceGateway.killTask(any(), any(), anyBoolean())).thenReturn(Observable.empty());
        client.doPOST("/api/v2/tasks/kill", new TaskKillCmd("myUser", "Titus1-1-1", null, true, false), null);

        verify(serviceGateway, times(1)).killTask("myUser", "Titus1-1-1", true);
    }

    @Test
    public void killMultipleTasks() throws Exception {
        List<TitusTaskInfo> tasks = createTitusTaskInfos(2);

        String tid0 = tasks.get(0).getId();
        String tid1 = tasks.get(1).getId();

        when(serviceGateway.findTaskById(tid0)).thenReturn(Observable.just(tasks.get(0)));
        when(serviceGateway.findTaskById(tid1)).thenReturn(Observable.just(tasks.get(1)));

        when(serviceGateway.killTask(any(), any(), anyBoolean())).thenReturn(Observable.empty());

        client.doPOST("/api/v2/tasks/kill", new TaskKillCmd("myUser", null, asList(tid0, tid1), true, false), null);

        verify(serviceGateway, times(1)).killTask("myUser", tid0, true);
        verify(serviceGateway, times(1)).killTask("myUser", tid1, true);
    }

    @Test
    public void killMultipleTasksInNonStrictMode() throws Exception {
        List<TitusTaskInfo> tasks = createTitusTaskInfos(2);

        String tid0 = tasks.get(0).getId();
        String tid1 = tasks.get(1).getId();

        when(serviceGateway.killTask("myUser", tid0, true)).thenReturn(Observable.empty());
        when(serviceGateway.killTask("myUser", tid1, true)).thenReturn(Observable.error(TitusServiceException.taskNotFound(tid1)));

        client.doPOST("/api/v2/tasks/kill", new TaskKillCmd("myUser", null, asList(tid0, tid1), true, false), null);

        verify(serviceGateway, times(0)).findTaskById(any());
        verify(serviceGateway, times(1)).killTask("myUser", tid0, true);
        verify(serviceGateway, times(1)).killTask("myUser", tid1, true);
    }

    @Test
    public void killMultipleTasksInStrictMode() throws Exception {
        TitusTaskInfo task = createTitusTaskInfos(1).get(0);

        try {
            when(serviceGateway.findTaskById(task.getId())).thenReturn(Observable.just(task));
            when(serviceGateway.findTaskById("missingTask")).thenReturn(Observable.error(new IllegalArgumentException("Task not found")));
            client.doPOST("/api/v2/tasks/kill", new TaskKillCmd("myUser", null, asList(task.getId(), "missingTask"), true, true), null);

            fail("Expected this request to fail");
        } catch (HttpTestClientException e) {
            assertThat(e.getStatusCode()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        }

        verify(serviceGateway, times(0)).killTask(any(), any(), anyBoolean());
    }

    @Test
    public void batchKillPartiallyFailedReportsPerTaskStatus() throws Exception {
        List<TitusTaskInfo> tasks = createTitusTaskInfos(2);

        String tid0 = tasks.get(0).getId();
        String tid1 = tasks.get(1).getId();

        when(serviceGateway.killTask("myUser", tid0, true)).thenReturn(Observable.empty());
        when(serviceGateway.killTask("myUser", tid1, true)).thenReturn(Observable.error(
                new RuntimeException("simulated error"))
        );

        try {
            client.doPOST("/api/v2/tasks/kill", new TaskKillCmd("myUser", null, asList(tid0, tid1), true, false), null);
            fail("REST call expected to fail");
        } catch (HttpTestClientException e) {
            assertThat(e.getContent()).isPresent().containsInstanceOf(ErrorResponse.class);
            ErrorResponse errorResponse = (ErrorResponse) e.getContent().get();

            assertThat(errorResponse.getErrorDetails()).isNotNull();
            Map<String, Object> details = (Map<String, Object>) errorResponse.getErrorDetails();

            assertThat(details).containsEntry("succeeded", singletonList(tid0));
            assertThat(details).containsEntry("failed", singletonMap(tid1, "simulated error"));
        }
    }

    private List<TitusTaskInfo> createTitusTaskInfos(int desired) {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "testJob"))
                .instancesDesired(desired)
                .instancesMax(desired)
                .build();
        String jobId = generator.newJobInfo(jobSpec).getId();
        generator.scheduleJob(jobId);
        return generator.getTitusTaskInfos(jobId);
    }
}