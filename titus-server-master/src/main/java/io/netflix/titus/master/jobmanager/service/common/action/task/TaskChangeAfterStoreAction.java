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

package io.netflix.titus.master.jobmanager.service.common.action.task;

import java.util.List;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange.Trigger;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

import static io.netflix.titus.common.framework.reconciler.ModelActionHolder.allModels;
import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions.updateTask;

/**
 * Persist new task first before updating reference model.
 */
public class TaskChangeAfterStoreAction extends TitusChangeAction {

    private final ReconciliationFramework<JobChange, JobManagerReconcilerEvent> reconciliationFramework;
    private final Function<Task, Task> changeFunction;
    private final JobStore titusStore;

    public TaskChangeAfterStoreAction(String taskId,
                                      ReconciliationFramework reconciliationFramework,
                                      Function<Task, Task> changeFunction,
                                      JobStore titusStore) {
        super(new JobChange(Trigger.Mesos, taskId, "Updating task state and writing it to the store"));
        this.reconciliationFramework = reconciliationFramework;
        this.changeFunction = changeFunction;
        this.titusStore = titusStore;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
        return Observable.fromCallable(() ->
                reconciliationFramework.findEngineByChildId(getChange().getId())
        ).flatMap(engineOpt -> engineOpt
                .map(engineTaskPair -> {
                    Task referenceTask = engineTaskPair.getRight().getEntity();
                    return apply(changeFunction.apply(referenceTask));
                })
                .orElseGet(() -> Observable.error(JobManagerException.taskNotFound(getChange().getId())))
        );
    }

    private Observable<Pair<JobChange, List<ModelActionHolder>>> apply(Task updatedTask) {
        return doStore(updatedTask).concatWith(Observable.just(
                Pair.of(getChange(), allModels(updateTask(updatedTask, getChange().getTrigger(), getChange().getSummary())))
        ));
    }

    private Observable<Pair<JobChange, List<ModelActionHolder>>> doStore(Task updatedTask) {
        return titusStore.storeTask(updatedTask).toObservable();
    }
}
