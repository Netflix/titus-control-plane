/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.jobactivity;

import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;

public class JobActivityPublisherRecordUtils {

    public static Job<?> getJobFromRecord(JobActivityPublisherRecord record)
            throws InvalidProtocolBufferException {
        com.netflix.titus.grpc.protogen.Job grpcJob = com.netflix.titus.grpc.protogen.Job.parseFrom(record.getSerializedEvent());
        return V3GrpcModelConverters.toCoreJob(grpcJob);
    }

    // TODO(Andrew L): The consumer of the serialized task record will need the job to complete two level resources.
    // The consumer should have a job it can reference or we can serialize that info here as well.
    public static Task getTaskFromRecord(Job<?> job, JobActivityPublisherRecord record)
            throws InvalidProtocolBufferException {
        com.netflix.titus.grpc.protogen.Task grpcTask = com.netflix.titus.grpc.protogen.Task.parseFrom(record.getSerializedEvent());
        return V3GrpcModelConverters.toCoreTask(job, grpcTask);
    }

    public static byte[] jobToByteArry(Job<?> job) {
        com.netflix.titus.grpc.protogen.Job grpcJob = V3GrpcModelConverters.toGrpcJob(job);
        return grpcJob.toByteArray();
    }

    public static byte[] taskToByteArray(Task task, LogStorageInfo<Task> logStorageInfo) {
        com.netflix.titus.grpc.protogen.Task grpcTask = V3GrpcModelConverters.toGrpcTask(task, logStorageInfo);
        return grpcTask.toByteArray();
    }
}
