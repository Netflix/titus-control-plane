/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodPhase;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodWrapper;
import io.kubernetes.client.openapi.models.V1Node;

import static com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.KillInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Launched;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.StartInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Started;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.isBefore;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_POD_SCHEDULED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_STUCK_IN_STATE;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_UNKNOWN;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LOST;

/**
 * Evaluates pod phase, pod condition and container state to determine best fitting Titus task state.
 */
public class PodToTaskMapper {

    @VisibleForTesting
    static final String TASK_STARTING = "TASK_STARTING";

    private final PodWrapper podWrapper;
    private final Optional<V1Node> node;
    private final Task task;
    private final ContainerResultCodeResolver containerResultCodeResolver;
    private final TitusRuntime titusRuntime;

    private final Either<TaskStatus, String> newTaskStatus;

    public PodToTaskMapper(PodWrapper podWrapper,
                           Optional<V1Node> node,
                           Task task,
                           boolean podDeleted,
                           ContainerResultCodeResolver containerResultCodeResolver,
                           TitusRuntime titusRuntime) {
        this.podWrapper = podWrapper;
        this.node = node;
        this.task = task;
        this.containerResultCodeResolver = containerResultCodeResolver;
        this.titusRuntime = titusRuntime;

        if (TaskState.isTerminalState(task.getStatus().getState())) {
            this.newTaskStatus = irrelevant("task already marked as finished");
        } else if (podDeleted) {
            this.newTaskStatus = handlePodDeleted();
        } else {
            switch (podWrapper.getPodPhase()) {
                case PENDING:
                    this.newTaskStatus = handlePodPending();
                    break;
                case RUNNING:
                    this.newTaskStatus = handlePodRunning();
                    break;
                case SUCCEEDED:
                case FAILED:
                    this.newTaskStatus = handlePodFinished();
                    break;
                case UNKNOWN:
                default:
                    this.newTaskStatus = unexpected("unknown pod phase");
            }
        }
    }

    /**
     * Handle pod object deleted event.
     */
    private Either<TaskStatus, String> handlePodDeleted() {
        TaskState taskState = task.getStatus().getState();
        boolean hasKillInitiatedState = JobFunctions.findTaskStatus(task, KillInitiated).isPresent();
        String reason = podWrapper.getReason();
        long now = titusRuntime.getClock().wallTime();

        if (!hasKillInitiatedState) {
            if (NODE_LOST.equals(reason)) {
                return Either.ofValue(TaskStatus.newBuilder()
                        .withState(Finished)
                        .withReasonCode(effectiveFinalReasonCode(REASON_TASK_KILLED))
                        .withReasonMessage("The host running the container was unexpectedly terminated")
                        .withTimestamp(now)
                        .build()
                );
            } else {
                return Either.ofValue(TaskStatus.newBuilder()
                        .withState(Finished)
                        .withReasonCode(effectiveFinalReasonCode(REASON_TASK_KILLED))
                        .withReasonMessage("Container was terminated without going through the Titus API")
                        .withTimestamp(now)
                        .build()
                );
            }
        }

        String reasonCode;
        if (podWrapper.getPodPhase() == PodPhase.PENDING || podWrapper.getPodPhase() == PodPhase.RUNNING) {
            // Pod in pending phase which is being deleted must have been terminated, as it was never run.
            // Pod in running state that did not complete must have been terminated as well.
            if (taskState == KillInitiated && task.getStatus().getReasonCode().equals(REASON_STUCK_IN_STATE)) {
                reasonCode = REASON_TRANSIENT_SYSTEM_ERROR;
            } else {
                reasonCode = REASON_TASK_KILLED;
            }
        } else if (podWrapper.getPodPhase() == PodPhase.SUCCEEDED) {
            reasonCode = resolveFinalTaskState(REASON_NORMAL);
        } else if (podWrapper.getPodPhase() == PodPhase.FAILED) {
            reasonCode = resolveFinalTaskState(REASON_FAILED);
        } else {
            titusRuntime.getCodeInvariants().inconsistent("Pod: %s has unknown phase mapping: %s", podWrapper.getName(), podWrapper.getPodPhase());
            reasonCode = REASON_UNKNOWN;
        }

        return Either.ofValue(TaskStatus.newBuilder()
                .withState(Finished)
                .withReasonCode(effectiveFinalReasonCode(reasonCode))
                .withReasonMessage(podWrapper.getMessage())
                .withTimestamp(now)
                .build()
        );
    }

    private String resolveFinalTaskState(String currentReasonCode) {
        TaskState taskState = task.getStatus().getState();
        if (taskState == KillInitiated && task.getStatus().getReasonCode().equals(REASON_STUCK_IN_STATE)) {
            return REASON_TRANSIENT_SYSTEM_ERROR;
        } else if (podWrapper.hasDeletionTimestamp() || taskState == KillInitiated) {
            return REASON_TASK_KILLED;
        }
        return currentReasonCode;
    }

    /**
     * Handle pod 'Pending' phase. In this phase we have to distinguish cases:
     * <ul>
     *     <li>pod is waiting in the queue</li>
     *     <li>pod is assigned to a node but not running yet</li>
     * </ul>
     */
    private Either<TaskStatus, String> handlePodPending() {
        if (!podWrapper.isScheduled()) {
            return handlePodPendingQueued();
        }
        return handlePodPendingInitializingInContainerWaitingState(podWrapper);
    }

    /**
     * Handle pod is waiting in the queue (is not scheduled yet). This event does not cause any task state
     * transition. We only verify consistency.
     */
    private Either<TaskStatus, String> handlePodPendingQueued() {
        if (task.getStatus().getState() != TaskState.Accepted) {
            return unexpected("expected queued task in the Accepted state");
        }
        return irrelevant("pod is waiting in the queue to be scheduled");
    }

    /**
     * Handle pod is assigned to a node but not running yet. The following pre-conditions must exist:
     * <ul>
     *     <li>pod condition 'PodScheduled' is set</li>
     *     <li>nodeName is not empty</li>
     * </ul>
     * Based on container state and reason message the state is classified as 'Launched' or 'StartInitiated':
     * <ul>
     *     <li>if reason != 'TASK_STARTING' set 'Launched'</li>
     *     <li>if reason == 'TASK_STARTING' set 'StartInitiated'</li>
     * </ul>
     * The state update happens only forward. Backward changes (StartInitiated -> Launched) are ignored.
     */
    private Either<TaskStatus, String> handlePodPendingInitializingInContainerWaitingState(PodWrapper podWrapper) {
        TaskState newState;

        // Pod that is being setup should be in 'Waiting' state.
        if (!podWrapper.hasContainerStateWaiting()) {
            newState = Launched;
        } else {
            String reason = podWrapper.getReason();

            // inspect pod status reason to differentiate between Launched and StartInitiated (this is not standard Kubernetes)
            if (reason.equalsIgnoreCase(TASK_STARTING)) {
                newState = StartInitiated;
            } else {
                newState = Launched;
            }
        }

        // Check for races. Do not allow setting back task state.
        if (isBefore(newState, task.getStatus().getState())) {
            return unexpected(String.format("pod in state not consistent with the task state (newState=%s)", newState));
        }

        String reason = podWrapper.getReason();
        return Either.ofValue(TaskStatus.newBuilder()
                .withState(newState)
                .withReasonCode(StringExt.isEmpty(reason) ? REASON_POD_SCHEDULED : reason)
                .withReasonMessage(podWrapper.getMessage())
                .withTimestamp(titusRuntime.getClock().wallTime())
                .build()
        );
    }

    /**
     * Handle pod 'Running.
     */
    private Either<TaskStatus, String> handlePodRunning() {
        TaskState taskState = task.getStatus().getState();
        // Check for races. Do not allow setting back task state.
        if (isBefore(Started, taskState)) {
            return unexpected("pod state (Running) not consistent with the task state");
        }
        return Either.ofValue(TaskStatus.newBuilder()
                .withState(Started)
                .withReasonCode(REASON_NORMAL)
                .withReasonMessage(podWrapper.getMessage())
                .withTimestamp(titusRuntime.getClock().wallTime())
                .build()
        );
    }

    /**
     * Handle pod 'Succeeded' or 'Failed'. Possible scenarios:
     * <ul>
     *     <li>pod terminated while sitting in a queue</li>
     *     <li>pod failure during container setup process (bad image, stuck in state, etc)</li>
     *     <li>pod terminated during container setup</li>
     *     <li>container ran to completion</li>
     *     <li>container terminated by a user</li>
     * </ul>
     */
    private Either<TaskStatus, String> handlePodFinished() {
        TaskState taskState = task.getStatus().getState();
        String reasonCode;
        if (podWrapper.getPodPhase() == PodPhase.SUCCEEDED) {
            reasonCode = resolveFinalTaskState(REASON_NORMAL);
        } else { // PodPhase.FAILED
            reasonCode = resolveFinalTaskState(REASON_FAILED);
        }
        return Either.ofValue(TaskStatus.newBuilder()
                .withState(Finished)
                .withReasonCode(effectiveFinalReasonCode(reasonCode))
                .withReasonMessage(podWrapper.getMessage())
                .withTimestamp(titusRuntime.getClock().wallTime())
                .build()
        );
    }

    public Either<TaskStatus, String> getNewTaskStatus() {
        return newTaskStatus;
    }

    /**
     * Pod notification irrelevant to the current task state.
     */
    private Either<TaskStatus, String> irrelevant(String message) {
        return Either.ofError(String.format("pod notification does not change task state (%s): digest=%s", message, digest()));
    }

    /**
     * Unexpected pod state.
     */
    private Either<TaskStatus, String> unexpected(String cause) {
        return Either.ofError(String.format("unexpected (%s); digest=%s", cause, digest()));
    }

    private String digest() {
        StringBuilder builder = new StringBuilder("{");

        // Task
        builder.append("taskId=").append(task.getId()).append(", ");
        builder.append("taskState=").append(task.getStatus().getState()).append(", ");

        // Pod
        builder.append("podPhase=").append(podWrapper.getPodPhase()).append(", ");
        builder.append("podReason=").append(podWrapper.getReason()).append(", ");
        builder.append("podMessage=").append(podWrapper.getMessage()).append(", ");
        builder.append("scheduled=").append(podWrapper.isScheduled()).append(", ");
        builder.append("deletionTimestamp=").append(podWrapper.hasDeletionTimestamp()).append(", ");

        // Node
        node.ifPresent(n -> builder.append("nodeId=").append(KubeUtil.getMetadataName(n.getMetadata())));

        return builder.append("}").toString();
    }

    private String effectiveFinalReasonCode(String reasonCode) {
        return containerResultCodeResolver.resolve(Finished, podWrapper.getMessage()).orElse(reasonCode);
    }
}
