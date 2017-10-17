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

import java.util.Collections;
import java.util.List;

import com.netflix.fenzo.queues.QAttributes;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction.Model;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.common.V3QAttributes;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import rx.Completable;
import rx.Observable;

/**
 */
public class WriteTaskAction extends TitusChangeAction {

    private final JobStore titusStore;
    private final SchedulingService schedulingService;
    private final ApplicationSlaManagementService capacityGroupService;

    private final Job<?> referenceJob;
    private final Task referenceTask;

    public WriteTaskAction(JobStore titusStore,
                           SchedulingService schedulingService,
                           ApplicationSlaManagementService capacityGroupService,
                           Job<?> referenceJob,
                           Task referenceTask) {
        super(new JobChange(ActionKind.Task, Trigger.Reconciler, referenceTask.getId(), "Persisting task to the store"));
        this.titusStore = titusStore;
        this.schedulingService = schedulingService;
        this.capacityGroupService = capacityGroupService;
        this.referenceJob = referenceJob;
        this.referenceTask = referenceTask;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        ModelUpdateAction updateAction = TitusModelUpdateActions.updateTask(referenceTask, Trigger.Reconciler, Model.Store, "Persisting task to the store");
        return titusStore.storeTask(referenceTask)
                .andThen(Completable.fromAction(this::removeFromFenzoFinishedTask))
                .andThen(Observable.just(Pair.of(getChange(), Collections.singletonList(updateAction))));
    }

    private void removeFromFenzoFinishedTask() {
        if (referenceTask.getStatus().getState() == TaskState.Finished) {
            Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(referenceJob, capacityGroupService);
            QAttributes qAttributes = new V3QAttributes(tierAssignment.getLeft().ordinal(), tierAssignment.getRight());
            String hostName = referenceTask.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "hostUnknown");
            schedulingService.removeTask(referenceTask.getId(), qAttributes, hostName);
        }
    }
}
