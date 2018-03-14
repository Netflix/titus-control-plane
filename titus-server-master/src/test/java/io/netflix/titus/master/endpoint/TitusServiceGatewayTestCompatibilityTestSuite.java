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

package io.netflix.titus.master.endpoint;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.api.service.TitusServiceException.ErrorCode;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import rx.Notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Set of compatibility tests for {@link TitusServiceGateway} implementations.
 */
public abstract class TitusServiceGatewayTestCompatibilityTestSuite<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE> {

    /**
     * A job containing in its name this string should be processed correctly.
     */
    protected static final String OK_JOB = "OkJob";

    /**
     * A job containing in its name this string should fail during submission.
     */
    protected static final String FAILED_TO_CREATE_JOB = "FailedToCreateJob";

    private TitusDataGenerator<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> dataGenerator;
    private TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> gateway;
    private EndpointModelAsserts<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> modelAsserts;
    private USER user;

    @Before
    public void setUp() throws Exception {
        dataGenerator = createDataGenerator();
        modelAsserts = createModelAsserts();
        gateway = createGateway();

        this.user = dataGenerator.createUser("testUser");
    }

    @Test
    public void testCreateBatchJob() throws Exception {
        testCreateJob(dataGenerator.createBatchJob(OK_JOB), dataGenerator.getBatchTypeQuery());
    }

    @Test
    public void testCreateServiceJob() throws Exception {
        testCreateJob(dataGenerator.createServiceJob(OK_JOB), dataGenerator.getServiceTypeQuery());
    }

    private void testCreateJob(JOB_SPEC jobSpec, JobQueryCriteria<TASK_STATE, JOB_TYPE> jobType) {
        String jobId = gateway.createJob(jobSpec).toBlocking().first();

        // Now find this job
        List<JOB> jobs = gateway.findJobsByCriteria(jobType, Optional.empty()).getLeft();
        assertThat(jobs).hasSize(1);

        JOB actualJob = jobs.get(0);
        modelAsserts.assertJobId(actualJob, jobId);
        modelAsserts.assertSpecOfJob(actualJob, jobSpec);
        modelAsserts.assertCellInfo(actualJob, getCellName());
    }

    @Test
    public void testFailureDuringJobCreationEmitsError() throws Exception {
        Notification<String> result = gateway.createJob(dataGenerator.createServiceJob(FAILED_TO_CREATE_JOB)).materialize().toBlocking().first();
        assertThat(result.getKind()).isEqualTo(Notification.Kind.OnError);
    }

    @Test
    public void testKillJob() {
        String jobId = gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();
        gateway.killJob(user, jobId).toBlocking().firstOrDefault(null);

        JOB killedJob = gateway.findJobById(jobId, false, Collections.emptySet()).toBlocking().first();
        modelAsserts.assertJobKilled(killedJob);
    }

    @Test
    public void testKillJobByMissingIdReturnsNoJobFoundStatusCode() throws Exception {
        Notification<Void> result = gateway.killJob(user, "missing_job_id").materialize().toBlocking().first();
        assertGatewayException(result, ErrorCode.JOB_NOT_FOUND);
    }

    @Test
    public void testFindById() throws Exception {
        String jobId = gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();
        JOB job = gateway.findJobById(jobId, false, Collections.emptySet()).toBlocking().first();
        modelAsserts.assertJobId(job, jobId);
    }

    @Test
    public void testFindByMissingIdReturnsNoJobFoundStatusCode() throws Exception {
        Notification<JOB> result = gateway.findJobById("missing_job_id", false, Collections.emptySet()).materialize().toBlocking().first();
        assertGatewayException(result, ErrorCode.JOB_NOT_FOUND);
    }

    @Test
    public void testFindAllActiveAndArchivedJobs() throws Exception {
        String archivedJobId = gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();
        gateway.killJob(user, archivedJobId).toBlocking().firstOrDefault(null);
        String activeJobId = gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();

        // Find active and archived
        List<JOB> allJobs = gateway.findJobsByCriteria(dataGenerator.getAllActiveAndArchivedQuery(), Optional.empty()).getLeft();

        assertJobInList(allJobs, archivedJobId);
        assertJobInList(allJobs, activeJobId);

        // Now find only archived
        List<JOB> activeJobs = gateway.findJobsByCriteria(dataGenerator.getAllActiveQuery(), Optional.empty()).getLeft();
        assertJobInList(activeJobs, activeJobId);
        assertJobNotInList(activeJobs, archivedJobId);
    }

    @Test
    public void testFindJobsWithLimitFilter() throws Exception {
        gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();
        gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();

        // Get single job
        List<JOB> singleJob = gateway.findJobsByCriteria(dataGenerator.getAllActiveWithLimitQuery(1), Optional.empty()).getLeft();
        assertThat(singleJob).hasSize(1);
    }

    @Test
    public void testFindTaskById() throws Exception {
        String jobId = gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();
        JOB job = gateway.findJobById(jobId, false, Collections.emptySet()).toBlocking().first();

        Set<String> taskIds = dataGenerator.getTaskIds(job);
        String firstTask = taskIds.iterator().next();

        TASK task = gateway.findTaskById(firstTask).toBlocking().first();
        assertThat(task).isNotNull();
    }

    @Test
    public void testFindTaskByMissingIdReturnsNoTaskFoundStatusCode() throws Exception {
        Notification<TASK> result = gateway.findTaskById("missing_task_id").materialize().toBlocking().first();
        assertGatewayException(result, ErrorCode.TASK_NOT_FOUND);
    }

    @Test
    public void testServiceJobResize() throws Exception {
        String jobId = gateway.createJob(dataGenerator.createServiceJob(OK_JOB)).toBlocking().first();
        gateway.resizeJob(user, jobId, 10, 5, 20).toBlocking().firstOrDefault(null);

        JOB job = gateway.findJobById(jobId, true, Collections.emptySet()).toBlocking().first();
        modelAsserts.assertServiceJobSize(job, 10, 5, 20);
    }

    @Test
    public void testServiceJobProcesses() throws Exception {
        String jobId = gateway.createJob(dataGenerator.createServiceJob(OK_JOB)).toBlocking().first();
        gateway.resizeJob(user, jobId, 10, 5, 20).toBlocking().firstOrDefault(null);
        JOB job = gateway.findJobById(jobId, true, Collections.emptySet()).toBlocking().first();
        modelAsserts.assertServiceJobSize(job, 10, 5, 20);

        gateway.updateJobProcesses(user, jobId, false, true).toBlocking().firstOrDefault(null);

        try {
            gateway.resizeJob(user, jobId, 15, 5, 20).toBlocking().firstOrDefault(null);
            Assertions.assertThat(true).isFalse();
        } catch (TitusServiceException ignoredException) {
        }

        // try decrease desired
        gateway.resizeJob(user, jobId, 8, 5, 20).toBlocking().firstOrDefault(null);
        job = gateway.findJobById(jobId, true, Collections.emptySet()).toBlocking().first();
        modelAsserts.assertServiceJobSize(job, 8, 5, 20);
    }

    @Test
    public void testServiceJobResizeByMissingIdReturnsNoJobFoundStatusCode() throws Exception {
        Notification<Void> result = gateway.resizeJob(user, "missing_job_id", 1, 1, 1).materialize().toBlocking().first();
        assertGatewayException(result, ErrorCode.JOB_NOT_FOUND);
    }

    @Test
    public void testChangeInServiceStatus() throws Exception {
        String jobId = gateway.createJob(dataGenerator.createServiceJob(OK_JOB)).toBlocking().first();
        gateway.changeJobInServiceStatus(user, jobId, false).toBlocking().firstOrDefault(null);

        JOB job = gateway.findJobById(jobId, true, Collections.emptySet()).toBlocking().first();
        modelAsserts.assertJobInService(job, false);
    }

    @Test
    public void testChangeInServiceStatusByMissingIdReturnsNoJobFoundStatusCode() throws Exception {
        Notification<Void> result = gateway.changeJobInServiceStatus(user, "missing_job_id", false).materialize().toBlocking().first();
        assertGatewayException(result, ErrorCode.JOB_NOT_FOUND);
    }

    @Test
    public void testChangeInServiceStatusNotAllowedForBatchJob() throws Exception {
        String jobId = gateway.createJob(dataGenerator.createBatchJob(OK_JOB)).toBlocking().first();
        Notification<Void> result = gateway.changeJobInServiceStatus(user, jobId, false).materialize().toBlocking().first();
        assertGatewayException(result, ErrorCode.UNSUPPORTED_JOB_TYPE);
    }

    private void assertGatewayException(Notification<?> result, ErrorCode errorCode) {
        assertThat(result.getKind()).isEqualTo(Notification.Kind.OnError);
        assertThat(result.getThrowable()).isInstanceOf(TitusServiceException.class);
        TitusServiceException e = (TitusServiceException) result.getThrowable();
        assertThat(e.getErrorCode()).isEqualTo(errorCode);
    }

    private void assertJobInList(List<JOB> allJobs, String jobId) {
        for (JOB job : allJobs) {
            try {
                modelAsserts.assertJobId(job, jobId);
                return;
            } catch (AssertionError ignore) {
            }
        }
        fail(jobId + " job not found in the query result");
    }

    private void assertJobNotInList(List<JOB> allJobs, String jobId) {
        try {
            assertJobInList(allJobs, jobId);
        } catch (AssertionError expected) {
            return;
        }
        fail(jobId + " was not supposed to be found the query result");
    }

    protected abstract TitusDataGenerator<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> createDataGenerator();

    protected abstract EndpointModelAsserts<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> createModelAsserts();

    protected abstract TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> createGateway();

    protected abstract String getCellName();
}
