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
import java.util.Optional;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.event.JobEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

import static java.util.Arrays.asList;

/**
 * Update runtime model to reflect last state of a task.
 */
public class TaskChangeAction extends TitusChangeAction {

    private final Function<Task, Task> changeFunction;

    public TaskChangeAction(String taskId, JobManagerEvent.Trigger trigger, Function<Task, Task> changeFunction, String changeSummary) {
        super(new JobChange(ActionKind.Task, trigger, taskId, "Updating task state (" + changeSummary + ')'));
        this.changeFunction = changeFunction;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        return Observable.just(Pair.of(
                getChange(),
                asList(
                        new TaskUpdateAction(ModelUpdateAction.Model.Reference),
                        new TaskUpdateAction(ModelUpdateAction.Model.Running)
                ))
        );
    }

    private class TaskUpdateAction extends TitusModelUpdateAction {

        public TaskUpdateAction(Model model) {
            super(ActionKind.Task, model, JobEvent.Trigger.Mesos, TaskChangeAction.this.getChange().getId(), "Updating task with function");
        }

        @Override
        public Pair<EntityHolder, Optional<EntityHolder>> apply(EntityHolder model) {
            return model.findById(getId())
                    .map(taskHolder -> {
                        Task oldTask = taskHolder.getEntity();
                        Task newTask = changeFunction.apply(oldTask);

                        if (newTask == oldTask) {
                            return Pair.of(model, Optional.<EntityHolder>empty());
                        }

                        if (oldTask instanceof ServiceJobTask) {
                            if (oldTask.getStatus().getState() == TaskState.KillInitiated
                                    && newTask.getStatus().getState() == TaskState.Finished
                                    && TaskStatus.REASON_SCALED_DOWN.equals(oldTask.getStatus().getReasonCode())) {

                                TaskStatus newStatus = newTask.getStatus().toBuilder().withReasonCode(TaskStatus.REASON_SCALED_DOWN).build();
                                newTask = JobFunctions.updateTaskStatus(newTask, newStatus);
                            }
                        }

                        EntityHolder newChild = EntityHolder.newRoot(oldTask.getId(), newTask);
                        EntityHolder newRoot = model.addChild(newChild);
                        return Pair.of(newRoot, Optional.of(newChild));
                    })
                    .orElseGet(() -> Pair.of(model, Optional.empty()));
        }

        @Override
        public String getSummary() {
            return "Updating task with function";
        }
    }
}
