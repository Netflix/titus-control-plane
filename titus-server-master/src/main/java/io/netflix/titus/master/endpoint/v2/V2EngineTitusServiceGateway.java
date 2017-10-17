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

package io.netflix.titus.master.endpoint.v2;

import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.api.service.TitusServiceException.ErrorCode;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.ApiOperations;
import io.netflix.titus.master.endpoint.TitusServiceGateway;
import io.netflix.titus.master.job.JobUpdateException;
import rx.Observable;

import static io.netflix.titus.master.endpoint.common.TitusServiceGatewayUtil.newObservable;

/**
 *
 */
public abstract class V2EngineTitusServiceGateway<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE>
        implements TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> {

    protected final ApiOperations apiOperations;

    public V2EngineTitusServiceGateway(ApiOperations apiOperations) {
        this.apiOperations = apiOperations;
    }

    @Override
    public Observable<Void> killTask(String user, String taskOrTaskInstanceId, boolean shrink) {
        return newObservable(subscriber -> {
            String taskId;
            try {
                taskId = WorkerNaming.getTaskId(findWorker(taskOrTaskInstanceId).getRight());
            } catch (Exception e) {
                subscriber.onError(e);
                return;
            }

            if (shrink) {
                try {
                    if (apiOperations.killWorkerAndShrink(taskId, user)) {
                        subscriber.onCompleted();
                    } else {
                        subscriber.onError(TitusServiceException.taskNotFound(taskId));
                    }
                } catch (JobUpdateException e) {
                    subscriber.onError(TitusServiceException.newBuilder(ErrorCode.JOB_UPDATE_NOT_ALLOWED, e.getMessage()).withCause(e).build());
                }
            } else {
                if (apiOperations.killWorker(taskId, user)) {
                    subscriber.onCompleted();
                } else {
                    subscriber.onError(TitusServiceException.taskNotFound(taskId));
                }
            }
        });
    }

    protected Pair<V2JobMetadata, V2WorkerMetadata> findWorker(String taskOrTaskInstanceId) {
        V2JobMetadata jobMetadata = null;
        V2WorkerMetadata mwmd = null;

        WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskOrTaskInstanceId);
        if (jobAndWorkerId.workerIndex == -1) {
            for (String jobId : apiOperations.getAllJobIds()) {
                V2JobMetadata nextJob = apiOperations.getJobMetadata(jobId);
                for (V2WorkerMetadata worker : apiOperations.getAllWorkers(jobId)) {
                    if (taskOrTaskInstanceId.equals(worker.getWorkerInstanceId())) {
                        jobMetadata = nextJob;
                        mwmd = worker;
                        break;
                    }
                }
            }
        } else {
            jobMetadata = apiOperations.getJobMetadata(jobAndWorkerId.jobId);
            if (jobMetadata != null) {
                mwmd = apiOperations.getWorker(jobAndWorkerId.jobId, jobAndWorkerId.workerNumber, true);
            }
        }
        if (jobMetadata == null) {
            throw TitusServiceException.taskNotFound(taskOrTaskInstanceId);
        }
        if (mwmd == null) {
            throw TitusServiceException.newBuilder(ErrorCode.TASK_NOT_FOUND, "Job " + jobMetadata.getJobId() + " has no task " + taskOrTaskInstanceId).build();
        }
        return Pair.of(jobMetadata, mwmd);
    }
}
