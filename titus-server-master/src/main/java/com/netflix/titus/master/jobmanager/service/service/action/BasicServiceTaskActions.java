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

package com.netflix.titus.master.jobmanager.service.service.action;

import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

public final class BasicServiceTaskActions {

    /**
     * Remove completed task from reconciler.
     */
    public static TitusChangeAction removeFinishedServiceTaskAction(JobStore jobStore, ServiceJobTask task) {
        return TitusChangeAction.newAction("removeFinishedServiceTaskAction")
                .task(task)
                .trigger(Trigger.Reconciler)
                .summary("Removing completed task from reconciler")
                .changeWithModelUpdates(self ->
                        jobStore.deleteTask(task).andThen(
                                Observable.just(ModelActionHolder.allModels(TitusModelAction.newModelUpdate(self).removeTask(task)))
                        ));
    }
}
