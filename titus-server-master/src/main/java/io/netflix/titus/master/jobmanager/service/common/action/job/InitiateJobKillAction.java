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

package io.netflix.titus.master.jobmanager.service.common.action.job;

import java.util.List;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction.Model;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Observable;

import static java.util.Arrays.asList;

/**
 * Persist new job status to the store and update running/store models.
 */
public class InitiateJobKillAction extends TitusChangeAction {

    private static final String SUMMARY = "Changing job state to KillInitiated and persisting it to store";

    private final ReconciliationEngine engine;
    private final JobStore titusStore;

    public InitiateJobKillAction(ReconciliationEngine engine, JobStore titusStore) {
        super(new JobChange(ActionKind.Job, Trigger.API, engine.getReferenceView().getId(), SUMMARY));
        this.engine = engine;
        this.titusStore = titusStore;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        Job job = engine.getReferenceView().getEntity();
        JobStatus newStatus = JobStatus.newBuilder()
                .withState(JobState.KillInitiated)
                .withReasonCode(TaskStatus.REASON_JOB_KILLED).withReasonMessage("External job termination request")
                .build();
        Job jobWithKillInitiated = JobFunctions.updateJobStatus(job, newStatus);

        List<ModelUpdateAction> updateActions = asList(
                TitusModelUpdateActions.updateJob(jobWithKillInitiated, Trigger.API, Model.Reference, SUMMARY),
                TitusModelUpdateActions.updateJob(jobWithKillInitiated, Trigger.API, Model.Store, SUMMARY),
                TitusModelUpdateActions.updateJob(jobWithKillInitiated, Trigger.API, Model.Running, SUMMARY)
        );

        return titusStore.updateJob(job).andThen(Observable.just(Pair.of(getChange(), updateActions)));
    }
}
