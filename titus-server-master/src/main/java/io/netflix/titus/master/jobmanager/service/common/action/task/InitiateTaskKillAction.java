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

import java.util.ArrayList;
import java.util.List;

import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.common.DifferenceResolverUtils;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange.Trigger;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Completable;
import rx.Observable;

/**
 */
public class InitiateTaskKillAction extends TitusChangeAction {

    private static final String SUMMARY = "Changing task state to KillInitiated and killing it on VM";

    private final Task task;
    private final boolean shrink;
    private final VirtualMachineMasterService vmService;
    private final String reasonCode;

    public InitiateTaskKillAction(Trigger trigger,
                                  Task task,
                                  boolean shrink,
                                  VirtualMachineMasterService vmService,
                                  String reasonCode,
                                  String cause) {
        super(new JobChange(trigger, task.getId(), SUMMARY + '(' + cause + ')'));
        this.task = task;
        this.shrink = shrink;
        this.vmService = vmService;
        this.reasonCode = reasonCode;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
        Task taskWithKillInitiated = JobFunctions.updateTaskStatus(task, TaskState.KillInitiated, reasonCode, getChange().getSummary());
        return killOnVM().concatWith(Observable.just(Pair.of(getChange(), createUpdateActions(taskWithKillInitiated))));
    }

    private Observable<Pair<JobChange, List<ModelActionHolder>>> killOnVM() {
        return Completable.fromAction(() -> vmService.killTask(task.getId())).toObservable();
    }

    private List<ModelActionHolder> createUpdateActions(Task taskWithKillInitiated) {
        List<ModelActionHolder> updateActions = new ArrayList<>();
        updateActions.addAll(ModelActionHolder.allModels(TitusModelUpdateActions.updateTask(taskWithKillInitiated, getChange().getTrigger(), SUMMARY)));
        if (shrink) {
            TitusModelUpdateAction shrinkAction = TitusModelUpdateActions.updateJob(
                    task.getJobId(),
                    job -> {
                        Job<ServiceJobExt> serviceJob = job;

                        ServiceJobExt oldExt = serviceJob.getJobDescriptor().getExtensions();

                        Capacity oldCapacity = oldExt.getCapacity();
                        int newDesired = oldCapacity.getDesired() - 1;
                        Capacity newCapacity = oldCapacity.toBuilder()
                                .withMin(Math.min(oldCapacity.getMin(), newDesired))
                                .withDesired(newDesired)
                                .build();

                        return serviceJob.toBuilder()
                                .withJobDescriptor(
                                        serviceJob.getJobDescriptor().toBuilder()
                                                .withExtensions(oldExt.toBuilder().withCapacity(newCapacity).build())
                                                .build())
                                .build();
                    },
                    getChange().getTrigger(),
                    "Shrinking job as a result of terminate and shrink request"
            );
            updateActions.add(ModelActionHolder.reference(shrinkAction));
        }
        return updateActions;
    }

    /**
     * Handle job kill initiated state
     */
    public static List<ChangeAction> applyKillInitiated(DifferenceResolverUtils.JobView<?, ?> runningJobView, VirtualMachineMasterService vmService) {
        List<ChangeAction> actions = new ArrayList<>();

        // If we have running tasks, terminate them first.
        if (!runningJobView.getTasks().isEmpty()) {
            for (Task task : runningJobView.getTasks()) {
                TaskState taskState = task.getStatus().getState();
                switch (taskState) {
                    case Accepted:
                    case Launched:
                    case StartInitiated:
                    case Started:
                        actions.add(new InitiateTaskKillAction(Trigger.Reconciler, task, false, vmService, TaskStatus.REASON_TASK_KILLED, "job kill initiated"));
                        break;
                    case KillInitiated:
                    case Disconnected:
                        // Do nothing until there is status update
                        break;
                    case Finished:
                        // Do nothing, as we will either remove it with job (when completed) or replace with a new task
                        break;
                }
            }
        }

        return actions;
    }
}
