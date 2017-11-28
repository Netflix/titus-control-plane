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

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange.Trigger;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import rx.Observable;

/**
 * Start a new task, by adding it to the scheduler.
 */
public class StartNewTaskAction extends TitusChangeAction {

    private final Job<?> job;
    private final Task task;
    private final ModelActionHolder action;

    private final ApplicationSlaManagementService capacityGroupService;
    private final SchedulingService schedulingService;

    public StartNewTaskAction(
            ApplicationSlaManagementService capacityGroupService,
            SchedulingService schedulingService,
            Job<?> job,
            Task task) {
        super(new JobChange(Trigger.Reconciler, task.getId(), "Starting new task"));
        this.capacityGroupService = capacityGroupService;
        this.schedulingService = schedulingService;
        this.job = job;

        this.task = task;
        this.action = ModelActionHolder.running(TitusModelUpdateActions.updateTask(task, Trigger.Reconciler, "Starting new task"));
    }

    @Override
    public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
        return Observable.fromCallable(() -> {
            Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(job, capacityGroupService);
            schedulingService.getTaskQueueAction().call(new V3QueueableTask(tierAssignment.getLeft(), tierAssignment.getRight(), job, task));
            return Pair.of(getChange(), Collections.singletonList(action));
        });
    }
}
