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

import java.util.Collections;
import java.util.List;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Observable;

/**
 */
public class WriteJobAction extends TitusChangeAction {
    private final JobStore titusStore;
    private final Job<BatchJobExt> job;

    public WriteJobAction(JobStore titusStore, Job<BatchJobExt> referenceJob) {
        super(new JobChange(ActionKind.Job, Trigger.Reconciler, referenceJob.getId(), "Writing job to the store"));
        this.titusStore = titusStore;
        this.job = referenceJob;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
        ModelActionHolder updateAction = ModelActionHolder.store(TitusModelUpdateActions.updateJob(job, Trigger.Reconciler, "Writing job to the store"));
        return titusStore.storeJob(job).andThen(Observable.just(Pair.of(getChange(), Collections.singletonList(updateAction))));
    }
}
