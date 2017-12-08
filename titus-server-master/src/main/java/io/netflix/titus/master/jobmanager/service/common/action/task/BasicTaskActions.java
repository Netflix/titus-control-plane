package io.netflix.titus.master.jobmanager.service.common.action.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.fenzo.queues.QAttributes;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.DateTimeExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.common.V3QAttributes;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.jobmanager.service.common.action.JobEntityHolders;
import io.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static io.netflix.titus.common.util.code.CodeInvariants.codeInvariants;
import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction.newModelUpdate;

public class BasicTaskActions {

    private static final Logger logger = LoggerFactory.getLogger(BasicTaskActions.class);

    /**
     * Update a task, and write it to store before updating reference and store models.
     * This action is used when handling user initiated updates.
     */
    public static TitusChangeAction updateTaskAndWriteItToStore(String taskId,
                                                                ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                                Function<Task, Task> changeFunction,
                                                                JobStore jobStore,
                                                                Trigger trigger,
                                                                String reason) {
        return TitusChangeAction.newAction("updateTaskAndWriteItToStore")
                .id(taskId)
                .trigger(trigger)
                .summary(reason)
                .changeWithModelUpdates(self ->
                        JobEntityHolders.expectTask(engine, taskId)
                                .map(task -> {
                                    Task newTask = changeFunction.apply(task);
                                    TitusModelAction modelUpdate = newModelUpdate(self).taskUpdate(newTask);
                                    return jobStore.updateTask(newTask).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelUpdate)));
                                })
                                .orElseGet(() -> Observable.error(JobManagerException.taskNotFound(taskId)))
                );
    }

    /**
     * Write updated task record to a store. If a task is completed, remove it from the scheduling service.
     * This command calls {@link JobStore#updateTask(Task)}, which assumes that the task record was created already.
     */
    public static TitusChangeAction writeReferenceTaskToStore(JobStore titusStore,
                                                              SchedulingService schedulingService,
                                                              ApplicationSlaManagementService capacityGroupService,
                                                              ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                              String taskId) {
        return TitusChangeAction.newAction("writeReferenceTaskToStore")
                .trigger(V3JobOperations.Trigger.Reconciler)
                .id(taskId)
                .summary("Persisting task to the store")
                .changeWithModelUpdate(self -> {
                    Optional<EntityHolder> taskHolder = engine.getReferenceView().findById(taskId);
                    if (!taskHolder.isPresent()) {
                        // Should never happen
                        codeInvariants().inconsistent("Reference task with id %s not found.", taskId);
                        return Observable.empty();
                    }
                    Task referenceTask = taskHolder.get().getEntity();

                    return titusStore.updateTask(referenceTask)
                            .andThen(Observable.fromCallable(() -> {
                                if (referenceTask.getStatus().getState() == TaskState.Finished) {
                                    Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(engine.getReferenceView().getEntity(), capacityGroupService);
                                    QAttributes qAttributes = new V3QAttributes(tierAssignment.getLeft().ordinal(), tierAssignment.getRight());
                                    String hostName = referenceTask.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "hostUnknown");
                                    schedulingService.removeTask(referenceTask.getId(), qAttributes, hostName);
                                }
                                TitusModelAction modelUpdateAction = newModelUpdate(self)
                                        .taskMaybeUpdate(storeRoot -> engine.getReferenceView().findById(taskId).map(current ->
                                                Pair.of(storeRoot.addChild(current), current))
                                        );
                                return ModelActionHolder.store(modelUpdateAction);
                            }));
                });
    }

    /**
     * Update a task in the reference and running models. If a task moves to Finished state, add retry delay information
     * to the task, and to task entity holder (see {@link TaskRetryers}).
     */
    public static TitusChangeAction updateTaskInRunningModel(String taskId,
                                                             Trigger trigger,
                                                             JobManagerConfiguration configuration,
                                                             ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                             Function<Task, Task> changeFunction,
                                                             String reason) {
        return TitusChangeAction.newAction("updateTaskInRunningModel")
                .id(taskId)
                .trigger(trigger)
                .summary(reason)
                .applyModelUpdates(self -> {
                            Optional<EntityHolder> taskOptional = JobEntityHolders.expectTaskHolder(engine, taskId);
                            if (!taskOptional.isPresent()) {
                                return Collections.emptyList();
                            }
                            EntityHolder taskHolder = taskOptional.get();
                            Task oldTask = taskHolder.getEntity();
                            Task newTask = changeFunction.apply(oldTask);

                            // In case of scale down request, copy reason code into Finished state.
                            if (oldTask instanceof ServiceJobTask) {
                                if (oldTask.getStatus().getState() == TaskState.KillInitiated
                                        && newTask.getStatus().getState() == TaskState.Finished
                                        && TaskStatus.REASON_SCALED_DOWN.equals(oldTask.getStatus().getReasonCode())) {

                                    TaskStatus newStatus = newTask.getStatus().toBuilder().withReasonCode(TaskStatus.REASON_SCALED_DOWN).build();
                                    newTask = JobFunctions.changeTaskStatus(newTask, newStatus);
                                }
                            }

                            // Handle separately reference and runtime models, as only reference model gets retry attributes.
                            List<ModelActionHolder> modelActionHolders = new ArrayList<>();

                            // Add ratyer data to task context.
                            EntityHolder newTaskHolder;
                            if (newTask.getStatus().getState() == TaskState.Finished) {
                                long retryDelayMs = TaskRetryers.getCurrentRetryerDelayMs(taskHolder, configuration.getTaskRetryerResetTimeMs());
                                String retryDelayString = DateTimeExt.toTimeUnitString(retryDelayMs);

                                newTask = newTask.toBuilder()
                                        .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_RETRY_DELAY, retryDelayString)
                                        .build();
                                newTaskHolder = taskHolder.
                                        setEntity(newTask)
                                        .addTag(TaskRetryers.ATTR_TASK_RETRY_DELAY_MS, retryDelayMs);

                                modelActionHolders.add(
                                        ModelActionHolder.reference(newModelUpdate(self)
                                                .summary("Setting retry delay on task in Finished state: %s", retryDelayString)
                                                .addTaskHolder(newTaskHolder))
                                );
                            } else {
                                modelActionHolders.add(ModelActionHolder.reference(newModelUpdate(self).taskUpdate(newTask)));
                            }

                    modelActionHolders.add(ModelActionHolder.running(newModelUpdate(self).taskUpdate(newTask)));

                            return modelActionHolders;
                        }
                );
    }

    /**
     * Add a task to {@link SchedulingService}, and create runtime entity holder for it.
     */
    public static TitusChangeAction scheduleTask(ApplicationSlaManagementService capacityGroupService,
                                                 SchedulingService schedulingService,
                                                 Job<?> job,
                                                 Task task) {
        return TitusChangeAction.newAction("scheduleTask")
                .task(task)
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary("Adding task to scheduler")
                .applyModelUpdate(self -> {
                    Pair<Tier, String> tierAssignment = JobManagerUtil.getTierAssignment(job, capacityGroupService);
                    schedulingService.getTaskQueueAction().call(new V3QueueableTask(tierAssignment.getLeft(), tierAssignment.getRight(), job, task));

                    TitusModelAction modelUpdateAction = newModelUpdate(self)
                            .summary("Creating new task entity holder")
                            .taskMaybeUpdate(jobHolder -> {
                                EntityHolder newTask = EntityHolder.newRoot(task.getId(), task);
                                EntityHolder newRoot = jobHolder.addChild(newTask);
                                return Optional.of(Pair.of(newRoot, newTask));
                            });
                    return ModelActionHolder.running(modelUpdateAction);
                });
    }
}
