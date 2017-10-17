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

package io.netflix.titus.testkit.perf.load.job;

import java.io.IOException;

import com.google.common.base.Preconditions;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class ServiceJobExecutor extends AbstractJobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ServiceJobExecutor.class);

    private volatile int currentMin;
    private volatile int currentDesired;
    private volatile int currentMax;

    public ServiceJobExecutor(TitusJobSpec jobSpec, ActiveJobsMonitor activeJobsMonitor, ExecutionContext context) {
        super(jobSpec, activeJobsMonitor, context);
        this.currentMin = jobSpec.getInstancesMin();
        this.currentDesired = jobSpec.getInstancesDesired();
        this.currentMax = jobSpec.getInstancesMax();
    }

    @Override
    public Observable<Void> terminateAndShrink(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getClient()
                .killTaskAndShrink(taskId)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to terminate and shrink task " + taskId + " of job " + name, e)))
                .doOnCompleted(() -> {
                    logger.info("Terminate and shrink succeeded for task {}", taskId);
                    this.currentDesired--;
                    logger.info("Instance count changed to min={}, desired={}, max={} of job {}", currentMin, currentDesired, currentMax, jobId);
                    jobReconciler.taskTerminateAndShrinkRequested(taskId).forEach(updateSubject::onNext);
                })
                .doOnSubscribe(() -> lastChangeTimestamp = -1)
                .doOnTerminate(() -> lastChangeTimestamp = System.currentTimeMillis());
    }

    @Override
    public Observable<Void> updateInstanceCount(int min, int desired, int max) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getClient()
                .setInstanceCount(new JobSetInstanceCountsCmd("loadRunner", jobId, desired, min, max))
                .onErrorResumeNext(e -> Observable.error(
                        new IOException("Failed to change instance count to min=" + min + ", desired=" + desired + ", max=" + max + " of job " + name, e)))
                .doOnCompleted(() -> {
                    logger.info("Instance count changed to min={}, desired={}, max={} of job {}", min, desired, max, jobId);
                    this.currentMin = min;
                    this.currentDesired = desired;
                    this.currentMax = max;
                    jobReconciler.updateInstanceCountRequested(jobId, min, desired, max).forEach(updateSubject::onNext);
                })
                .doOnSubscribe(() -> lastChangeTimestamp = -1)
                .doOnTerminate(() -> lastChangeTimestamp = System.currentTimeMillis());
    }

    @Override
    public Observable<Void> scaleUp(int delta) {
        return updateInstanceCount(currentMin, currentDesired + delta, currentMax);
    }

    @Override
    public Observable<Void> scaleDown(int delta) {
        return updateInstanceCount(currentMin, currentDesired - delta, currentMax);
    }
}
