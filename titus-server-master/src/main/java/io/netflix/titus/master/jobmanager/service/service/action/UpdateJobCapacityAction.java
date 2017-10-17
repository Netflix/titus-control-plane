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

package io.netflix.titus.master.jobmanager.service.service.action;

import java.util.Collections;
import java.util.List;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Observable;

public class UpdateJobCapacityAction extends TitusChangeAction {

    private final ReconciliationEngine engine;
    private final Capacity capacity;

    public UpdateJobCapacityAction(ReconciliationEngine engine, Capacity capacity) {
        super(new JobChange(ActionKind.Job, JobManagerEvent.Trigger.API, engine.getReferenceView().getId(), "Job resize operation requested"));
        this.engine = engine;
        this.capacity = capacity;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        Job<ServiceJobExt> job = engine.getReferenceView().getEntity();

        JobDescriptor<ServiceJobExt> jobDescriptor = job.getJobDescriptor().toBuilder()
                .withExtensions(job.getJobDescriptor().getExtensions().toBuilder()
                        .withCapacity(capacity)
                        .build()
                )
                .build();

        Job<ServiceJobExt> updatedJob = job.toBuilder().withJobDescriptor(jobDescriptor).build();

        return Observable.just(Pair.of(
                getChange(),
                Collections.singletonList(
                        TitusModelUpdateActions.updateJob(
                                updatedJob,
                                JobManagerEvent.Trigger.API,
                                ModelUpdateAction.Model.Reference,
                                "Job resize operation requested"
                        )
                )
        ));
    }
}
