package com.netflix.titus.runtime.connector.jobmanager.replicator;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultJobDataReplicator extends StreamDataReplicator<JobSnapshot> implements JobDataReplicator {

    private static final String JOB_REPLICATOR = "jobReplicator";
    private static final String JOB_REPLICATOR_RETRYABLE_STREAM = "jobReplicatorRetryableStream";
    private static final String JOB_REPLICATOR_GRPC_STREAM = "jobReplicatorGrpcStream";

    @Inject
    public DefaultJobDataReplicator(JobManagementClient client, TitusRuntime titusRuntime) {
        super(
                newReplicatorEventStream(client, titusRuntime),
                new DataReplicatorMetrics(JOB_REPLICATOR, titusRuntime),
                titusRuntime
        );
    }

    private static RetryableReplicatorEventStream<JobSnapshot> newReplicatorEventStream(JobManagementClient client, TitusRuntime titusRuntime) {
        GrpcJobReplicatorEventStream grpcEventStream = new GrpcJobReplicatorEventStream(
                client,
                new DataReplicatorMetrics(JOB_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.computation()
        );
        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new DataReplicatorMetrics(JOB_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.computation()
        );
    }
}
