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

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class ServiceJobExecutor extends AbstractJobExecutor<ServiceJobExt> {

    private static final Logger logger = LoggerFactory.getLogger(ServiceJobExecutor.class);

    private volatile Capacity currentCapacity;

    private ServiceJobExecutor(Job<ServiceJobExt> job, ExecutionContext context) {
        super(job, context);
        this.currentCapacity = job.getJobDescriptor().getExtensions().getCapacity();
    }

    @Override
    public Observable<Void> terminateAndShrink(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient().killTask(taskId, true)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to terminate and shrink task " + taskId + " of job " + name, e)))
                .doOnCompleted(() -> {
                    logger.info("Terminate and shrink succeeded for task {}", taskId);
                    this.currentCapacity = currentCapacity.toBuilder().withDesired(currentCapacity.getDesired() - 1).build();
                    logger.info("Job capacity changed: jobId={}, capacity={}", jobId, currentCapacity);
                });
    }

    @Override
    public Observable<Void> updateInstanceCount(int min, int desired, int max) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient().updateJobSize(jobId, min, desired, max)
                .onErrorResumeNext(e -> Observable.error(
                        new IOException("Failed to change instance count to min=" + min + ", desired=" + desired + ", max=" + max + " of job " + name, e)))
                .doOnCompleted(() -> {
                    logger.info("Instance count changed to min={}, desired={}, max={} of job {}", min, desired, max, jobId);
                    this.currentCapacity = Capacity.newBuilder().withMin(min).withDesired(desired).withMax(max).build();
                });
    }

    @Override
    public Observable<Void> scaleUp(int delta) {
        return updateInstanceCount(currentCapacity.getMin(), currentCapacity.getDesired() + delta, currentCapacity.getMax());
    }

    @Override
    public Observable<Void> scaleDown(int delta) {
        return updateInstanceCount(currentCapacity.getMin(), currentCapacity.getDesired() - delta, currentCapacity.getMax());
    }

    public static Observable<ServiceJobExecutor> submitJob(JobDescriptor<ServiceJobExt> jobSpec, ExecutionContext context) {
        return context.getJobManagementClient()
                .createJob(V3GrpcModelConverters.toGrpcJobDescriptor(jobSpec))
                .flatMap(jobRef -> context.getJobManagementClient().findJob(jobRef))
                .map(job -> new ServiceJobExecutor((Job<ServiceJobExt>) V3GrpcModelConverters.toCoreJob(job), context));
    }
}
