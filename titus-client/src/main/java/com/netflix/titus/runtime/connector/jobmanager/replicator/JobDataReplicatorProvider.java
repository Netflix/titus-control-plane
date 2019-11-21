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

package com.netflix.titus.runtime.connector.jobmanager.replicator;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicator;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorDelegate;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import reactor.core.scheduler.Schedulers;

@Singleton
public class JobDataReplicatorProvider implements Provider<JobDataReplicator> {

    private static final String JOB_REPLICATOR = "jobReplicator";
    private static final String JOB_REPLICATOR_RETRYABLE_STREAM = "jobReplicatorRetryableStream";
    private static final String JOB_REPLICATOR_GRPC_STREAM = "jobReplicatorGrpcStream";

    private static final long JOB_BOOTSTRAP_TIMEOUT_MS = 120_000;

    private final JobDataReplicatorImpl replicator;

    @Inject
    public JobDataReplicatorProvider(JobManagementClient client, TitusRuntime titusRuntime) {
        this(client, Collections.emptyMap(), titusRuntime);
    }

    public JobDataReplicatorProvider(JobManagementClient client, Map<String, String> filteringCriteria, TitusRuntime titusRuntime) {
        StreamDataReplicator<JobSnapshot, JobManagerEvent<?>> original = StreamDataReplicator.newStreamDataReplicator(
                newReplicatorEventStream(client, filteringCriteria, titusRuntime),
                new JobDataReplicatorMetrics(JOB_REPLICATOR, titusRuntime),
                titusRuntime
        ).blockFirst(Duration.ofMillis(JOB_BOOTSTRAP_TIMEOUT_MS));

        this.replicator = new JobDataReplicatorImpl(original);
    }

    @PreDestroy
    public void shutdown() {
        ExceptionExt.silent(replicator::close);
    }

    @Override
    public JobDataReplicator get() {
        return replicator;
    }

    private static RetryableReplicatorEventStream<JobSnapshot, JobManagerEvent<?>> newReplicatorEventStream(JobManagementClient client,
                                                                                                            Map<String, String> filteringCriteria,
                                                                                                            TitusRuntime titusRuntime) {
        GrpcJobReplicatorEventStream grpcEventStream = new GrpcJobReplicatorEventStream(
                client,
                filteringCriteria,
                new JobDataReplicatorMetrics(JOB_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new JobDataReplicatorMetrics(JOB_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    private static class JobDataReplicatorImpl extends DataReplicatorDelegate<JobSnapshot, JobManagerEvent<?>> implements JobDataReplicator {
        JobDataReplicatorImpl(DataReplicator<JobSnapshot, JobManagerEvent<?>> delegate) {
            super(delegate);
        }
    }

    private static class JobDataReplicatorMetrics extends DataReplicatorMetrics<JobSnapshot, JobManagerEvent<?>> {

        private JobDataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
            super(source, titusRuntime);
        }

        @Override
        public void event(ReplicatorEvent<JobSnapshot, JobManagerEvent<?>> event) {
            super.event(event);
            setCacheCollectionSize("jobs", event.getSnapshot().getJobs().size());
            setCacheCollectionSize("tasks", event.getSnapshot().getTasks().size());
        }
    }
}
