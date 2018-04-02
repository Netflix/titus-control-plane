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

package com.netflix.titus.testkit.perf.load.job;

import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import rx.Observable;

public class BatchJobExecutor extends AbstractJobExecutor {

    private BatchJobExecutor(Job<BatchJobExt> job, ExecutionContext context) {
        super(job, context);
    }

    @Override
    public Observable<Void> terminateAndShrink(String taskId) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Observable<Void> updateInstanceCount(int min, int desired, int max) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Observable<Void> scaleUp(int delta) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Observable<Void> scaleDown(int delta) {
        throw new IllegalStateException("Not supported");
    }

    public static Observable<BatchJobExecutor> submitJob(JobDescriptor<BatchJobExt> jobSpec, ExecutionContext context) {
        return context.getJobManagementClient()
                .createJob(V3GrpcModelConverters.toGrpcJobDescriptor(jobSpec))
                .flatMap(jobRef -> context.getJobManagementClient().findJob(jobRef))
                .map(job -> new BatchJobExecutor((Job<BatchJobExt>) V3GrpcModelConverters.toCoreJob(job), context));
    }
}
