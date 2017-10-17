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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.event.JobEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

/**
 */
public class CompleteJobAction extends TitusChangeAction {
    private static final String SUMMARY = "Moving job to Finished state";

    public CompleteJobAction(String jobId) {
        super(new JobChange(ActionKind.Job, JobManagerEvent.Trigger.Reconciler, jobId, SUMMARY));
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        return Observable.just(Pair.of(getChange(),
                Arrays.asList(
                        new JobStateUpdateAction(ModelUpdateAction.Model.Reference),
                        new JobStateUpdateAction(ModelUpdateAction.Model.Running)
                ))
        );
    }

    private class JobStateUpdateAction extends TitusModelUpdateAction {

        public JobStateUpdateAction(Model model) {
            super(ActionKind.Job, model, JobEvent.Trigger.Reconciler, CompleteJobAction.this.getChange().getId(), "Updating job state");
        }

        @Override
        public Pair<EntityHolder, Optional<EntityHolder>> apply(EntityHolder model) {
            Job job = model.getEntity();
            if (job.getStatus().getState() != JobState.Finished) {
                JobStatus newStatus = JobModel.newJobStatus()
                        .withState(JobState.Finished)
                        .withReasonCode(TaskStatus.REASON_NORMAL)
                        .build();
                Job newJob = JobFunctions.updateJobStatus(job, newStatus);
                EntityHolder newRoot = model.setEntity(newJob);
                return Pair.of(newRoot, Optional.of(newRoot));
            }
            return Pair.of(model, Optional.empty());
        }
    }
}
