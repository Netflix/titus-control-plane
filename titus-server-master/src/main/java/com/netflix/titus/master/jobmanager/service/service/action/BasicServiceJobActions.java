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

import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

public class BasicServiceJobActions {

    /**
     * Update job capacity.
     */
    public static TitusChangeAction updateJobCapacityAction(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                            CapacityAttributes capacityAttributes,
                                                            JobStore jobStore,
                                                            CallMetadata callMetadata,
                                                            EntitySanitizer entitySanitizer) {
        return TitusChangeAction.newAction("updateJobCapacityAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job capacity to: %s", capacityAttributes)
                .callMetadata(callMetadata)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();

                    if (serviceJob.getStatus().getState() != JobState.Accepted) {
                        return Observable.error(JobManagerException.jobTerminating(serviceJob));
                    }

                    Capacity currentCapacity = serviceJob.getJobDescriptor().getExtensions().getCapacity();
                    Capacity.Builder newCapacityBuilder = currentCapacity.toBuilder();
                    capacityAttributes.getDesired().ifPresent(newCapacityBuilder::withDesired);
                    capacityAttributes.getMax().ifPresent(newCapacityBuilder::withMax);
                    capacityAttributes.getMin().ifPresent(newCapacityBuilder::withMin);
                    if (capacityAttributes.getDesired().isPresent()) {
                        newCapacityBuilder.withDesired(capacityAttributes.getDesired().get());
                    } else {
                        setDesiredBasedOnMinMax(newCapacityBuilder, currentCapacity, capacityAttributes);
                    }
                    Capacity newCapacity = newCapacityBuilder.build();

                    if (currentCapacity.equals(newCapacity)) {
                        return Observable.empty();
                    }

                    // model validation for capacity
                    Set<ValidationError> violations = entitySanitizer.validate(newCapacity);
                    if (!violations.isEmpty()) {
                        return Observable.error(TitusServiceException.invalidArgument(
                                String.format("Current %s", currentCapacity),
                                violations
                        ));
                    }

                    // checking if service job processes allow changes to desired capacity
                    if (isDesiredCapacityInvalid(newCapacity, serviceJob)) {
                        return Observable.error(JobManagerException.invalidDesiredCapacity(serviceJob.getId(), newCapacity.getDesired(),
                                serviceJob.getJobDescriptor().getExtensions().getServiceJobProcesses()));
                    }

                    // ready to update job capacity
                    Job<ServiceJobExt> updatedJob = JobFunctions.changeServiceJobCapacity(serviceJob, newCapacity);
                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

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
                .callMetadata(callMetadata)
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
                .callMetadata(callMetadata)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    if (serviceJob.getStatus().getState() != JobState.Accepted) {
                        return Observable.error(JobManagerException.jobTerminating(serviceJob));
                    }

                    Job<ServiceJobExt> updatedJob = JobFunctions.changeServiceJobProcesses(serviceJob, processes);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Automatically adjust the desired size to be within the bounds of (min,max). This method assumes min <= max.
     */
    private static void setDesiredBasedOnMinMax(Capacity.Builder builder, Capacity current, CapacityAttributes update) {
        update.getMin().ifPresent(min -> {
            if (min > current.getDesired()) {
                builder.withDesired(min);
            }
        });
        update.getMax().ifPresent(max -> {
            if (max < current.getDesired()) {
                builder.withDesired(max);
            }
        });
    }

    private static boolean isDesiredCapacityInvalid(Capacity targetCapacity, Job<ServiceJobExt> serviceJob) {
        ServiceJobProcesses serviceJobProcesses = serviceJob.getJobDescriptor().getExtensions().getServiceJobProcesses();
        Capacity currentCapacity = serviceJob.getJobDescriptor().getExtensions().getCapacity();
        return (serviceJobProcesses.isDisableIncreaseDesired() && targetCapacity.getDesired() > currentCapacity.getDesired()) ||
                (serviceJobProcesses.isDisableDecreaseDesired() && targetCapacity.getDesired() < currentCapacity.getDesired());

    }
}
