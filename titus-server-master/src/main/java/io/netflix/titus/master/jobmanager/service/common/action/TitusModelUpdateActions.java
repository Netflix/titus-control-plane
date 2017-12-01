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

package io.netflix.titus.master.jobmanager.service.common.action;

import java.util.Optional;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.util.tuple.Pair;

@Deprecated
public final class TitusModelUpdateActions {

    public static TitusModelAction updateJob(Job job, Trigger trigger, String summary) {
        return new UpdateJobAction(job.getId(), jh -> jh.setEntity(job), trigger, summary);
    }

    public static TitusModelAction updateJob(String jobId, Function<Job, Job> updateFun, Trigger trigger, String summary) {
        return new UpdateJobAction(jobId, applyJob(updateFun), trigger, summary);
    }

    public static TitusModelAction updateJobHolder(String jobId, Function<EntityHolder, EntityHolder> updateFun, Trigger trigger, String summary) {
        return new UpdateJobAction(jobId, updateFun, trigger, summary);
    }

    public static TitusModelAction createTask(Task task, Trigger trigger, String summary) {
        return new UpdateTaskAction(task, trigger, summary);
    }

    public static TitusModelAction removeTask(String taskId, Trigger trigger, String summary) {
        return new RemoveTaskAction(taskId, trigger, summary);
    }

    private static Function<EntityHolder, EntityHolder> applyJob(Function<Job, Job> jobHolderFun) {
        return jobHolder -> jobHolder.setEntity(jobHolderFun.apply(jobHolder.getEntity()));
    }

    private static class UpdateJobAction extends TitusModelAction {

        private final Function<EntityHolder, EntityHolder> updateFun;

        UpdateJobAction(String jobId, Function<EntityHolder, EntityHolder> updateFun, Trigger trigger, String summary) {
            super(trigger, jobId, summary);
            this.updateFun = updateFun;
        }

        @Override
        public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
            EntityHolder newRoot = updateFun.apply(rootHolder);
            return Optional.of(Pair.of(newRoot, newRoot));
        }
    }

    private static class UpdateTaskAction extends TitusModelAction {

        private final Task task;

        UpdateTaskAction(Task task, Trigger trigger, String summary) {
            super(trigger, task.getId(), summary);
            this.task = task;
        }

        @Override
        public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
            EntityHolder newTask = EntityHolder.newRoot(task.getId(), task);
            EntityHolder newRoot = rootHolder.addChild(newTask);
            return Optional.of(Pair.of(newRoot, newTask));
        }
    }

    private static class RemoveTaskAction extends TitusModelAction {

        RemoveTaskAction(String taskId, Trigger trigger, String summary) {
            super(trigger, taskId, summary);
        }

        @Override
        public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
            Pair<EntityHolder, Optional<EntityHolder>> result = rootHolder.removeChild(getId());
            return result.getRight().map(task -> Pair.of(result.getLeft(), task));
        }
    }
}
