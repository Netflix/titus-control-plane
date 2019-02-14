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

package com.netflix.titus.master.jobmanager.service.service.action;

import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import jdk.nashorn.internal.codegen.CompilerConstants;
import rx.Observable;

public class BasicServiceJobActions {

    /**
     * Update job capacity.
     */
    public static TitusChangeAction updateJobCapacityAction(ReconciliationEngine<JobManagerReconcilerEvent> engine, Capacity capacity, JobStore jobStore, CallMetadata callMetadata) {
        return TitusChangeAction.newAction("updateJobCapacityAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job capacity to: %s", capacity)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    if (serviceJob.getStatus().getState() != JobState.Accepted) {
                        return Observable.error(JobManagerException.jobTerminating(serviceJob));
                    }

                    Job<ServiceJobExt> updatedJob = JobFunctions.changeServiceJobCapacity(serviceJob, capacity);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob).addTag("CALLMETADATA", callMetadata));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Change job 'enable' status.
     */
    public static TitusChangeAction updateJobEnableStatus(ReconciliationEngine<JobManagerReconcilerEvent> engine, boolean enabled, JobStore jobStore, CallMetadata callMetadata) {
        return TitusChangeAction.newAction("updateJobCapacityAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job enable status to: %s", enabled)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    if (serviceJob.getStatus().getState() != JobState.Accepted) {
                        return Observable.error(JobManagerException.jobTerminating(serviceJob));
                    }

                    Job<ServiceJobExt> updatedJob = JobFunctions.changeJobEnabledStatus(serviceJob, enabled);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Change job service processes configuration.
     */
    public static TitusChangeAction updateServiceJobProcesses(ReconciliationEngine<JobManagerReconcilerEvent> engine, ServiceJobProcesses processes, JobStore jobStore, CallMetadata callMetadata) {
        return TitusChangeAction.newAction("updateServiceJobProcesses")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job service processes to: %s", processes)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    if (serviceJob.getStatus().getState() != JobState.Accepted) {
                        return Observable.error(JobManagerException.jobTerminating(serviceJob));
                    }

                    Job<ServiceJobExt> updatedJob = JobFunctions.changeServiceJobProcesses(serviceJob, processes);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob).addTag("CALLMETADATA", callMetadata));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }
}
