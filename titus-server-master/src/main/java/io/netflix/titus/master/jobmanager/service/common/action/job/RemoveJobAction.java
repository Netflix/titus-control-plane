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

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Observable;

/**
 * Remove a job.
 */
public class RemoveJobAction extends TitusChangeAction {

    private static final String SUMMARY = "Removing job from the storage";

    private final JobStore store;
    private final Job job;

    public RemoveJobAction(Job job, JobStore store) {
        super(new JobChange(ActionKind.Job, JobManagerEvent.Trigger.Reconciler, job.getId(), SUMMARY));
        this.store = store;
        this.job = job;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
        return store.deleteJob(job)
                .andThen(Observable.just(Pair.of(getChange(),
                        Collections.singletonList(ModelActionHolder.reference(TitusModelUpdateActions.closeJob(getChange().getId())))
                )));
    }
}
