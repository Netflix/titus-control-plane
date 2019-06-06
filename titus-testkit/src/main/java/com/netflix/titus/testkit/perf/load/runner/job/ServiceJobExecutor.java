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

package com.netflix.titus.testkit.perf.load.runner.job;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static com.netflix.titus.testkit.perf.load.runner.LoadGeneratorConstants.TEST_CALL_METADATA;

public class ServiceJobExecutor extends AbstractJobExecutor<ServiceJobExt> {

    private static final Logger logger = LoggerFactory.getLogger(ServiceJobExecutor.class);

    private final AtomicReference<Capacity> currentCapacity;

    private ServiceJobExecutor(Job<ServiceJobExt> job, ExecutionContext context) {
        super(job, context);
        this.currentCapacity = new AtomicReference<>(job.getJobDescriptor().getExtensions().getCapacity());
    }

    @Override
    public Mono<Void> terminateAndShrink(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient().killTask(taskId, true, TEST_CALL_METADATA)
                .onErrorResume(e -> {
                    Status.Code code = Status.fromThrowable(e).getCode();
                    if (code.equals(Status.Code.NOT_FOUND) || code.equals(Status.Code.FAILED_PRECONDITION)) {
                        logger.info("Task {} not found while terminating it, or it was already not running - ignoring", taskId);
                        return Mono.empty();
                    }
                    return Mono.error(new IOException("Failed to terminate and shrink task " + taskId + " of job " + name, e));
                })
                .doOnSuccess(ignored -> {
                    logger.info("Terminate and shrink succeeded for task {}", taskId);
                    Capacity newCapacity = this.currentCapacity.updateAndGet(current ->
                            current.toBuilder().withDesired(current.getDesired() - 1).build()
                    );
                    logger.info("Job capacity changed: jobId={}, capacity={}", jobId, newCapacity);
                });
    }

    @Override
    public Mono<Void> updateInstanceCount(int min, int desired, int max) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        Capacity capacity = Capacity.newBuilder()
                .withMin(min)
                .withDesired(desired)
                .withMax(max)
                .build();
        return context.getJobManagementClient()
                .updateJobCapacity(jobId, capacity, TEST_CALL_METADATA)
                .onErrorResume(e -> Mono.error(
                        new IOException("Failed to change instance count to min=" + min + ", desired=" + desired + ", max=" + max + " of job " + name, e))
                )
                .doOnSuccess(ignored -> {
                    logger.info("Instance count changed to min={}, desired={}, max={} of job {}", min, desired, max, jobId);
                    this.currentCapacity.set(capacity);
                });
    }

    @Override
    public Mono<Void> scaleUp(int delta) {
        Capacity capacity = currentCapacity.get();
        return updateInstanceCount(capacity.getMin(), Math.min(capacity.getMax(), capacity.getDesired() + delta), capacity.getMax());
    }

    @Override
    public Mono<Void> scaleDown(int delta) {
        Capacity capacity = currentCapacity.get();
        return updateInstanceCount(capacity.getMin(), Math.max(capacity.getMin(), capacity.getDesired() - delta), capacity.getMax());
    }

    public static Mono<ServiceJobExecutor> submitJob(JobDescriptor<ServiceJobExt> jobSpec, ExecutionContext context) {
        return context.getJobManagementClient()
                .createJob(jobSpec, TEST_CALL_METADATA)
                .flatMap(jobRef -> context.getJobManagementClient().findJob(jobRef))
                .map(job -> new ServiceJobExecutor((Job<ServiceJobExt>) job, context));
    }
}
