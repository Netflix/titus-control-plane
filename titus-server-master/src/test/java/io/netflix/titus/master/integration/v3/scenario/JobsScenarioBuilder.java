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

package io.netflix.titus.master.integration.v3.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import io.netflix.titus.testkit.grpc.TestStreamObserver;

import static io.netflix.titus.common.util.ExceptionExt.rethrow;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.TIMEOUT_MS;

/**
 */
public class JobsScenarioBuilder {

    private final JobManagementServiceGrpc.JobManagementServiceStub client;

    private final List<JobScenarioBuilder> jobScenarioBuilders = new ArrayList<>();
    private final EmbeddedTitusOperations titusOperations;

    public JobsScenarioBuilder(EmbeddedTitusOperations titusOperations) {
        this.titusOperations = titusOperations;
        this.client = titusOperations.getV3GrpcClient();
        jobScenarioBuilders.addAll(loadJobs());
    }

    public void stop() {
        jobScenarioBuilders.forEach(JobScenarioBuilder::stop);
    }

    public JobsScenarioBuilder schedule(JobDescriptor jobDescriptor,
                                        Function<JobScenarioBuilder, JobScenarioBuilder> jobScenario) throws Exception {
        TestStreamObserver<JobId> responseObserver = new TestStreamObserver<>();
        client.createJob(V3GrpcModelConverters.toGrpcJobDescriptor(jobDescriptor), responseObserver);

        JobId jobId = responseObserver.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
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

    public JobsScenarioBuilder assertJobs(Predicate<List<Job>> predicate) {
        List<Job> jobs = jobScenarioBuilders.stream().map(JobScenarioBuilder::getJob).collect(Collectors.toList());
        Preconditions.checkState(predicate.test(jobs), "Jobs collection predicate evaluation fails (job size=%s)", jobs.size());
        return this;
    }

    private List<JobScenarioBuilder> loadJobs() {
        TestStreamObserver<JobQueryResult> responseObserver = new TestStreamObserver<>();
        JobQuery query = JobQuery.newBuilder().setPage(Page.newBuilder().setPageSize(1000)).build();
        client.findJobs(query, responseObserver);

        JobQueryResult queryResult = rethrow(() -> responseObserver.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        List<JobScenarioBuilder> result = new ArrayList<>();
        queryResult.getItemsList().forEach(job -> {
            result.add(new JobScenarioBuilder(titusOperations, this, job.getId()));
        });

        return result;
    }
}
