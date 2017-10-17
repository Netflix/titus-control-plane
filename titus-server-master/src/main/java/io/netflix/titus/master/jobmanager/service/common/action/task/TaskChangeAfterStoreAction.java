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

import io.netflix.titus.api.jobmanager.model.event.JobEvent;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Observable;

import static java.util.Arrays.asList;

/**
 * Persist new task first before updating reference model.
 */
public class TaskChangeAfterStoreAction extends TitusChangeAction {

    private final ReconciliationFramework<JobChange> reconciliationFramework;
    private final Function<Task, Task> changeFunction;
    private final JobStore titusStore;

    public TaskChangeAfterStoreAction(String taskId,
                                      ReconciliationFramework reconciliationFramework,
                                      Function<Task, Task> changeFunction,
                                      JobStore titusStore) {
        super(new JobChange(ActionKind.Task, JobEvent.Trigger.Mesos, taskId, "Updating task state and writing it to the store"));
        this.reconciliationFramework = reconciliationFramework;
        this.changeFunction = changeFunction;
        this.titusStore = titusStore;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
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

    private Observable<Pair<JobChange, List<ModelUpdateAction>>> apply(Task updatedTask) {
        return doStore(updatedTask).concatWith(Observable.just(
                Pair.of(
                        getChange(),
                        asList(
                                TitusModelUpdateActions.updateTask(updatedTask, getChange().getTrigger(), ModelUpdateAction.Model.Reference, getChange().getSummary()),
                                TitusModelUpdateActions.updateTask(updatedTask, getChange().getTrigger(), ModelUpdateAction.Model.Store, getChange().getSummary()),
                                TitusModelUpdateActions.updateTask(updatedTask, getChange().getTrigger(), ModelUpdateAction.Model.Running, getChange().getSummary())
                        )
                )
        ));
    }

    private Observable<Pair<JobChange, List<ModelUpdateAction>>> doStore(Task updatedTask) {
        return titusStore.storeTask(updatedTask).toObservable();
    }
}
