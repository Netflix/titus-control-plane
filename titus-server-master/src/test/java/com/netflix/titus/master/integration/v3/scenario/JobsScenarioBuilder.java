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

package com.netflix.titus.master.integration.v3.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.ExceptionExt.rethrow;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.TIMEOUT_MS;

/**
 *
 */
public class JobsScenarioBuilder extends ExternalResource {

    private static final Logger logger = LoggerFactory.getLogger(JobsScenarioBuilder.class);

    private final TitusStackResource titusStackResource;
    private final TitusMasterResource titusMasterResource;

    private EmbeddedTitusOperations titusOperations;
    private JobManagementServiceGrpc.JobManagementServiceStub client;

    private final List<JobScenarioBuilder> jobScenarioBuilders = new ArrayList<>();

    public JobsScenarioBuilder(TitusStackResource titusStackResource) {
        this.titusStackResource = titusStackResource;
        this.titusMasterResource = null;
    }

    public JobsScenarioBuilder(TitusMasterResource titusMasterResource) {
        this.titusStackResource = null;
        this.titusMasterResource = titusMasterResource;
    }

    public JobsScenarioBuilder(EmbeddedTitusOperations titusOperations) {
        this.titusStackResource = null;
        this.titusMasterResource = null;
        this.titusOperations = titusOperations;
        try {
            before();
        } catch (Throwable error) {
            throw new IllegalStateException(error);
        }
    }

    @Override
    protected void before() throws Throwable {
        if (titusStackResource != null) {
            this.titusOperations = titusStackResource.getOperations();
        }
        if (titusMasterResource != null) {
            this.titusOperations = titusMasterResource.getOperations();
        }
        this.client = titusOperations.getV3GrpcClient();
        this.jobScenarioBuilders.addAll(loadJobs());
    }

    @Override
    protected void after() {
        stop();
    }

    public void stop() {
        jobScenarioBuilders.forEach(JobScenarioBuilder::stop);
    }

    public JobsScenarioBuilder schedule(JobDescriptor jobDescriptor,
                                        Function<JobScenarioBuilder, JobScenarioBuilder> jobScenario) {
        TestStreamObserver<JobId> responseObserver = new TestStreamObserver<>();
        client.withDeadlineAfter(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .createJob(GrpcJobManagementModelConverters.toGrpcJobDescriptor(jobDescriptor), responseObserver);

        JobId jobId;
        try {
            jobId = responseObserver.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        Preconditions.checkNotNull(jobId, "Job create operation not completed in time");

        TestStreamObserver<JobChangeNotification> eventStream = new TestStreamObserver<>();
        client.observeJob(jobId, eventStream);

        JobScenarioBuilder jobScenarioBuilder = new JobScenarioBuilder(titusOperations, this, jobId.getId());
        jobScenarioBuilders.add(jobScenarioBuilder);

        jobScenario.apply(jobScenarioBuilder);

        return this;
    }

    public JobsScenarioBuilder schedule(JobDescriptor<?> jobDescriptor,
                                        int times,
                                        Function<JobScenarioBuilder, JobScenarioBuilder> jobScenario) throws Exception {
        String sequence = jobDescriptor.getJobGroupInfo().getSequence();
        for (int i = 0; i < times; i++) {
            int finalI = i;
            JobDescriptor effective = sequence.isEmpty()
                    ? jobDescriptor
                    : jobDescriptor.but(jd -> jd.getJobGroupInfo().toBuilder().withSequence(sequence + '_' + finalI));
            schedule(effective, jobScenario);
        }
        return this;
    }

    public Job scheduleAndReturnJob(JobDescriptor jobDescriptor,
                                    Function<JobScenarioBuilder, JobScenarioBuilder> jobScenario) {
        AtomicReference<Job> jobRef = new AtomicReference<>();
        schedule(jobDescriptor, js -> jobScenario.apply(js).inJob(jobRef::set));

        Preconditions.checkNotNull(jobRef.get(), "Job not set after scheduling");
        return jobRef.get();
    }

    public JobScenarioBuilder takeJob(String jobId) {
        return jobScenarioBuilders.stream().filter(j -> j.getJobId().equals(jobId)).findFirst().orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
    }

    public JobScenarioBuilder takeJob(int idx) {
        Preconditions.checkArgument(idx < jobScenarioBuilders.size(), "Invalid job index: %s (max=%s)", idx, jobScenarioBuilders.size());
        return jobScenarioBuilders.get(idx);
    }

    public String takeJobId(int idx) {
        return takeJob(idx).getJobId();
    }

    public String takeTaskId(int jobIdx, int taskIdx) {
        return takeJob(jobIdx).getTaskByIndex(taskIdx).getTask().getId();
    }

    public String takeTaskId(String jobId, int taskIdx) {
        return takeJob(jobId).getTaskByIndex(taskIdx).getTask().getId();
    }

    public JobsScenarioBuilder assertJobs(Predicate<List<Job>> predicate) {
        List<Job> jobs = jobScenarioBuilders.stream().map(JobScenarioBuilder::getJob).collect(Collectors.toList());
        Preconditions.checkState(predicate.test(jobs), "Jobs collection predicate evaluation fails (job size=%s)", jobs.size());
        return this;
    }

    public void expectVersionsOrdered() {
        jobScenarioBuilders.forEach(JobScenarioBuilder::expectVersionsOrdered);
    }

    private List<JobScenarioBuilder> loadJobs() {
        JobQuery query = JobQuery.newBuilder().setPage(Page.newBuilder().setPageSize(1000)).build();
        Throwable lastFailure = null;

        // During TitusMaster reboot we reuse the same ephemeral GRPC port with the same client side ManagedChannel
        // in TitusGateway. The channel may have broken connection which after reboot my result in a failure here, and
        // since we do not have retry interceptor installed, we make a few attempts directly.
        for (int i = 0; i < 3; i++) {
            try {
                TestStreamObserver<JobQueryResult> responseObserver = new TestStreamObserver<>();
                client.findJobs(query, responseObserver);

                JobQueryResult queryResult = rethrow(() -> responseObserver.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS));

                List<JobScenarioBuilder> result = new ArrayList<>();
                queryResult.getItemsList().forEach(job -> {
                    result.add(new JobScenarioBuilder(titusOperations, this, job.getId()));
                });

                return result;
            } catch (Exception e) {
                lastFailure = e;
                logger.info("Cannot load jobs from TitusMaster (might be not ready yet). Waiting 1sec before next try...");
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException ignore) {
                }
            }
        }
        throw new IllegalStateException("Cannot load jobs: " + lastFailure, lastFailure);
    }
}
