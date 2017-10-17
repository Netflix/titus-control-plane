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

package io.netflix.titus.master.job.batch;

import java.io.IOException;
import java.util.List;

import com.google.inject.Injector;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.master.JobSchedulingInfo;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.BaseJobMgr;
import io.netflix.titus.master.job.JobManagerConfiguration;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.worker.WorkerRequest;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.store.InvalidNamedJobException;
import io.netflix.titus.master.store.NamedJob;
import io.netflix.titus.master.store.NamedJobs;
import io.netflix.titus.master.store.V2JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.subjects.ReplaySubject;

public class BatchJobMgr extends BaseJobMgr {

    private static final Logger logger = LoggerFactory.getLogger(BatchJobMgr.class);

    private static final String ROOT_METRICS_NAME = MetricConstants.METRIC_SCHEDULING_JOB + "batch.";

    public BatchJobMgr(Injector injector,
                       String jobId,
                       V2JobDefinition jobDefinition,
                       NamedJob namedJob,
                       Observer<Observable<JobSchedulingInfo>> jobSchedulingObserver,
                       MasterConfiguration config,
                       JobManagerConfiguration jobManagerConfiguration,
                       JobConfiguration jobConfiguration,
                       RxEventBus eventBus,
                       SchedulingService schedulingService,
                       Registry registry) {
        super(injector,
                jobId,
                false,
                jobDefinition,
                namedJob,
                jobSchedulingObserver,
                config,
                jobManagerConfiguration,
                jobConfiguration,
                logger,
                ROOT_METRICS_NAME,
                eventBus,
                schedulingService,
                registry
        );
    }

    public static V2JobMgrIntf acceptSubmit(String user, V2JobDefinition jobDefinition, NamedJobs namedJobs,
                                            Injector injector,
                                            ReplaySubject<Observable<JobSchedulingInfo>> jobSchedulingObserver,
                                            RxEventBus eventBus, Registry registry, V2JobStore store) {
        Parameters.JobType jobType = Parameters.getJobType(jobDefinition.getParameters());
        if (jobType != Parameters.JobType.Batch) {
            throw new IllegalArgumentException("Can't accept " + jobType + " job definition");
        }
        try {
            String jobId = createNewJobid(jobDefinition, namedJobs);
            final SchedulingService schedulingService = injector.getInstance(SchedulingService.class);
            V2JobMgrIntf jobMgr = new BatchJobMgr(injector, jobId, jobDefinition,
                    namedJobs.getJobByName(jobDefinition.getName()), jobSchedulingObserver,
                    injector.getInstance(MasterConfiguration.class), injector.getInstance(JobManagerConfiguration.class),
                    injector.getInstance(JobConfiguration.class),
                    eventBus, schedulingService, registry);
            jobMgr.initializeNewJob(store);
            return jobMgr;
        } catch (InvalidNamedJobException | InvalidJobException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static String createNewJobid(V2JobDefinition jobDefinition, NamedJobs namedJobs) throws InvalidNamedJobException {
        // TODO make this work with the namedJob created for this job definiiton
        final NamedJobs.JobIdForSubmit jobIdForSubmit = namedJobs.getJobIdForSubmit(jobDefinition.getName(), jobDefinition);
        return jobIdForSubmit.getJobId();
    }

    protected boolean shouldResubmit(V2WorkerMetadata mwmd, V2JobState newState) {
        if (V2JobState.isTerminalState(mwmd.getState())) {
            return false;
        }
        V2JobMetadata job = getJobMetadata();
        switch (newState) {
            case Failed:
                if (mwmd.getTotalResubmitCount() >= job.getSla().getRetries()) {
                    numWorkerResubmitLimitReached.increment();
                    return false;
                }
                resubmitRateLimiter.delayWorkerResubmit(jobId, mwmd.getStageNum(), mwmd.getWorkerIndex());
                return true;
            case Completed:
                return false;
        }
        return false;
    }

    @Override
    public void updateInstances(int stageNum, int min, int desired, int max, String user) throws InvalidJobException {
        throw new InvalidJobException(jobId + ": can't change instance counts for batch job");
    }

    @Override
    public boolean killTaskAndShrink(String taskId, String user) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    protected void createAllWorkers() throws InvalidJobException {
        List<WorkerRequest> workers = getInitialWorkers();
        if (workers == null || workers.isEmpty()) {
            return;
        }
        int beg = 0;
        while (true) {
            if (beg >= workers.size()) {
                break;
            }
            int en = beg + Math.min(workerWritesBatchSize, workers.size() - beg);
            try {
                List<? extends V2WorkerMetadata> newWorkers = store.storeNewWorkers(workers.subList(beg, en));
                newWorkers.forEach(w -> {
                    queueTask(w);
                    jobMetrics.updateTaskMetrics(w);
                });
            } catch (InvalidJobException e) {
                // TODO confirm if this is possible, likely not
                throw new InvalidJobException("Unexpected error: " + e.getMessage(), e);
            } catch (IOException e) {
                logger.error("Error storing workers of job " + jobId + " - " + e.getMessage(), e);
            }
            beg = en;
        }
    }

    @Override
    public void enforceSla() {
        V2JobMetadata jobMetadata = getJobMetadata();
        if (jobMetadata == null || V2JobState.isTerminalState(jobMetadata.getState())) {
            return;
        }
        final long now = System.currentTimeMillis();
        final long limit = jobMetadata.getSla().getRuntimeLimitSecs() * 1000L;
        if (limit > 0L) {
            getAllActiveWorkers(jobMetadata).stream()
                    .filter(t -> t.getState() == V2JobState.Started)
                    .forEach(t -> {
                        if ((now - t.getStartedAt()) > limit) {
                            logger.info(jobId + ": " + getTaskStringPrefix(t.getWorkerIndex(), t.getWorkerNumber()) +
                                    " killing - reached runtime limit of " + jobMetadata.getSla().getRuntimeLimitSecs() + " secs");
                            killWorker(t, this.getClass().getSimpleName(), "runtime limit reached", true, false);
                        }
                    });
        }
    }
}
