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

package com.netflix.titus.master.jobmanager.service.common.action;

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

/**
 * Functions for processing job manager {@link EntityHolder} instances.
 */
public final class JobEntityHolders {

    public static Optional<EntityHolder> expectTaskHolder(ReconciliationEngine<JobManagerReconcilerEvent> engine, String taskId, TitusRuntime titusRuntime) {
        Optional<EntityHolder> taskHolder = engine.getReferenceView().findById(taskId);
        if (taskHolder.isPresent()) {
            return Optional.of(taskHolder.get());
        }
        titusRuntime.getCodeInvariants().inconsistent("Expected to find task %s owned by job %s", taskId, engine.getReferenceView().getId());
        return Optional.empty();
    }

    public static Optional<Task> expectTask(ReconciliationEngine<JobManagerReconcilerEvent> engine, String taskId, TitusRuntime titusRuntime) {
        return expectTaskHolder(engine, taskId, titusRuntime).map(EntityHolder::getEntity);
    }

    public static Observable<Task> toTaskObservable(ReconciliationEngine<JobManagerReconcilerEvent> engine, String taskId, TitusRuntime titusRuntime) {
        return Observable.fromCallable(() -> expectTask(engine, taskId, titusRuntime).orElse(null)).filter(Objects::nonNull);
    }

    public static Pair<EntityHolder, EntityHolder> addTask(EntityHolder rootHolder, Task newTask) {
        EntityHolder newTaskHolder = rootHolder.findById(newTask.getId())
                .map(taskHolder -> taskHolder.setEntity(newTask))
                .orElseGet(() -> EntityHolder.newRoot(newTask.getId(), newTask));
        return Pair.of(rootHolder.addChild(newTaskHolder), newTaskHolder);
    }
}
