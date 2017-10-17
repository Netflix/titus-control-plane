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

package io.netflix.titus.master.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerAssignments;
import io.netflix.titus.api.model.v2.WorkerHost;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.JobSchedulingInfo;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.store.V2JobMetadataWritable;
import io.netflix.titus.master.store.V2JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

class JobAssignmentsPublisher {
    private static final Logger logger = LoggerFactory.getLogger(JobAssignmentsPublisher.class);
    private final String jobId;
    private Map<Integer, WorkerAssignments> stageAssignments = new HashMap<>();
    private BehaviorSubject<JobSchedulingInfo> behaviorSubject = null;
    private V2JobStore store;

    JobAssignmentsPublisher(String jobId,
                            Observer<Observable<JobSchedulingInfo>> jobSchedulingObserver,
                            Observable<Status> statusObservable) {
        this.jobId = jobId;
        if (jobSchedulingObserver != null) {
            behaviorSubject = BehaviorSubject.create(
                    new JobSchedulingInfo(jobId, new HashMap<Integer, WorkerAssignments>()));
            logger.info("Added behavior subject of job " + jobId + " to schedulingObserver");
            jobSchedulingObserver.onNext(this.behaviorSubject);
            statusObservable
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends Status>>() {
                        @Override
                        public Observable<? extends Status> call(Throwable throwable) {
                            logger.warn("Couldn't send status to assignmentChangeObservers: " + throwable.getMessage());
                            return Observable.empty();
                        }
                    })
                    .subscribe(new Action1<Status>() {
                        @Override
                        public void call(Status status) {
                            handleStatus(status);
                        }
                    });
        }
    }

    void init(V2JobStore store) {
        this.store = store;
    }

    void init(V2JobStore store, Map<Integer, WorkerAssignments> stageAssignments) {
        this.store = store;
        this.stageAssignments.clear();
        this.stageAssignments.putAll(stageAssignments);
        if (behaviorSubject != null) {
            behaviorSubject.onNext(new JobSchedulingInfo(jobId, stageAssignments));
        }
    }

    void handleStatus(Status status) {
        V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) store.getActiveJob(jobId);
        if (jobMetadata == null || V2JobState.isTerminalState(jobMetadata.getState())) {
            behaviorSubject.onCompleted();
            return;
        }
        if (status.getWorkerNumber() == -1) {
            return;
        }
        V2WorkerMetadata mwmd = null;
        V2StageMetadata msmd = null;
        try {
            mwmd = JobMgrUtils.getV2WorkerMetadataWritable(jobMetadata, status.getWorkerNumber());
            msmd = jobMetadata.getStageMetadata(mwmd.getStageNum());
        } catch (InvalidJobException e) {
            try {
                mwmd = JobMgrUtils.getArchivedWorker(store, jobId, status.getWorkerNumber());
                if (mwmd == null) {
                    logger.warn("Unexpected to not find worker " + status.getWorkerNumber() + " for job " + jobId + ", state=" + status.getState());
                } else {
                    logger.warn("Skipping setting assignments for archived worker, state=" + status.getState());
                }
            } catch (IOException e1) {
                logger.warn("Can't get archive worker number " + status.getWorkerNumber() + " for job " + jobId, e1);
            }
            return;
        }
        status.setStageNum(mwmd.getStageNum());
        status.setWorkerIndex(mwmd.getWorkerIndex());

        WorkerAssignments assignments = stageAssignments.get(status.getStageNum());
        if (assignments == null) {
            assignments = new WorkerAssignments(status.getStageNum(), msmd.getNumWorkers(), new HashMap<Integer, WorkerHost>());
            stageAssignments.put(status.getStageNum(), assignments);
        }
        assignments.setNumWorkers(msmd.getNumWorkers());
        Map<Integer, WorkerHost> hosts = assignments.getHosts();
        hosts.put(mwmd.getWorkerNumber(), new WorkerHost(mwmd.getSlave(), mwmd.getWorkerIndex(), mwmd.getPorts(),
                status.getState(), mwmd.getWorkerNumber()));

        //logger.info("********************* Sending scheduling change to behavior subject due to " + status.getState() +
        //        ": " + stageAssignments);
        if (behaviorSubject != null) {
            behaviorSubject.onNext(new JobSchedulingInfo(jobId, new HashMap<>(stageAssignments)));
        }
        // remove from WorkerHost any workers that are in terminal state, notification went out for them once.
        Set<Integer> keysToRem = new HashSet<>();
        for (Map.Entry<Integer, WorkerHost> entry : assignments.getHosts().entrySet()) {
            if (V2JobState.isTerminalState(entry.getValue().getState())) {
                keysToRem.add(entry.getKey());
            }
        }
        for (Integer k : keysToRem) {
            assignments.getHosts().remove(k);
        }
    }

    WorkerAssignments getSink() {
        V2JobMetadata mjmd = store.getActiveJob(jobId);
        return stageAssignments.get(mjmd.getNumStages());
    }

    void stop() {
        behaviorSubject.onCompleted();
    }
}
