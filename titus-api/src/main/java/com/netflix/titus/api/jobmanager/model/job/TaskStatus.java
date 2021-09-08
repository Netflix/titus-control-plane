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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

/**
 *
 */
@ClassFieldsNotNull
public class TaskStatus extends ExecutableStatus<TaskState> {

    public static final String REASON_NORMAL = "normal";

    /**
     * Filled in to preserve state history continuity.
     */
    public static final String REASON_STATE_MISSING = "filledIn";

    /**
     * Task has a pod created for it (set in the Accepted state only).
     */
    public static final String REASON_POD_CREATED = "podCreated";

    /**
     * Pod scheduled by KubeScheduler.
     */
    public static final String REASON_POD_SCHEDULED = "podScheduled";

    /**
     * Job was explicitly terminated by a user.
     */
    public static final String REASON_JOB_KILLED = "killed";

    /**
     * Task was explicitly terminated by a user.
     */
    public static final String REASON_TASK_KILLED = "killed";

    /**
     * Task was terminated by an eviction service.
     */
    public static final String REASON_TASK_EVICTED = "evicted";

    /**
     * Task was lost, and its final status is unknown.
     */
    public static final String REASON_TASK_LOST = "lost";

    /**
     * Invalid container definition (security group, image name, etc).
     */
    public static final String REASON_INVALID_REQUEST = "invalidRequest";

    /**
     * Task was terminated as a result of job scaling down.
     */
    public static final String REASON_SCALED_DOWN = "scaledDown";

    /**
     * Task was terminated, as it did not progress to the next state in the expected time.
     */
    public static final String REASON_STUCK_IN_STATE = "stuckInState";

    /**
     * Task which was in KillInitiated state, was terminated, as it did not progress to the Finished state in the expected time.
     */
    public static final String REASON_STUCK_IN_KILLING_STATE = "stuckInKillingState";

    /**
     * Task was terminated, as its runtime limit was exceeded.
     */
    public static final String REASON_RUNTIME_LIMIT_EXCEEDED = "runtimeLimitExceeded";

    /**
     * Task completed with non zero error code.
     */
    public static final String REASON_FAILED = "failed";

    /**
     * Container crashed due to some internal system error.
     */
    public static final String REASON_CRASHED = "crashed";

    /**
     * Transient error, not an agent specific (for example AWS rate limiting).
     */
    public static final String REASON_TRANSIENT_SYSTEM_ERROR = "transientSystemError";

    /**
     * An error scoped to an agent instance on which a container was run. The agent should be quarantined or terminated.
     */
    public static final String REASON_LOCAL_SYSTEM_ERROR = "localSystemError";

    /**
     * Unrecognized error which cannot be classified neither as local/non-local or transient.
     * If there are multiple occurrences of this error, the agent should be quarantined or terminated.
     */
    public static final String REASON_UNKNOWN_SYSTEM_ERROR = "unknownSystemError";

    public static final String REASON_UNKNOWN = "unknown";

    private static Set<String> SYSTEM_LEVEL_ERRORS = CollectionsExt.asSet(
            REASON_STUCK_IN_STATE,
            REASON_CRASHED,
            REASON_TRANSIENT_SYSTEM_ERROR,
            REASON_LOCAL_SYSTEM_ERROR,
            REASON_UNKNOWN_SYSTEM_ERROR
    );
    
    private final List<ContainerState> containerStates;

    public TaskStatus(TaskState taskState, String reasonCode, String reasonMessage, long timestamp) {
        this(taskState, reasonCode, reasonMessage, timestamp, Collections.emptyList());
    }

    public TaskStatus(TaskState taskState, String reasonCode, String reasonMessage, long timestamp, List<ContainerState> containerStates) {
        super(taskState, reasonCode, reasonMessage, timestamp);
        this.containerStates = containerStates;
    }

    public List<ContainerState> getContainerStates() {
        if (containerStates == null) {
            return Collections.emptyList();
        }
        return containerStates;
    }

    public static boolean hasSystemError(Task task) {
        if (isSystemError(task.getStatus())) {
            return true;
        }

        return JobFunctions.findTaskStatus(task, TaskState.KillInitiated).map(TaskStatus::isSystemError)
                .orElse(false);
    }

    public static boolean isSystemError(TaskStatus status) {
        if (status.getState() != TaskState.KillInitiated && status.getState() != TaskState.Finished) {
            return false;
        }
        String reasonCode = status.getReasonCode();
        return !StringExt.isEmpty(reasonCode) && SYSTEM_LEVEL_ERRORS.contains(reasonCode);
    }

    public static boolean isEvicted(Task task) {
        if (task.getStatus().getState() != TaskState.Finished) {
            return false;
        }
        return JobFunctions.findTaskStatus(task, TaskState.KillInitiated)
                .map(killInitiatedState ->
                        REASON_TASK_EVICTED.equals(killInitiatedState.getReasonCode())
                ).orElse(false);
    }

    /**
     * Checks if task has a status (Accepted, reasonCode=podCreated). A matching task will have at least two
     * Accepted states in its history, so all of them must be checked.
     */
    public static boolean hasPod(Task task) {
        if (isPodMarker(task.getStatus())) {
            return true;
        }
        for (TaskStatus status : task.getStatusHistory()) {
            if (isPodMarker(status)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPodMarker(TaskStatus status) {
        return status.getState() == TaskState.Accepted && REASON_POD_CREATED.equals(status.getReasonCode());
    }

    public static boolean areEquivalent(TaskStatus first, TaskStatus second) {
        if (first.getState() != second.getState()) {
            return false;
        }
        if (!Objects.equals(first.getReasonCode(), second.getReasonCode())) {
            return false;
        }
        return Objects.equals(first.getReasonMessage(), second.getReasonMessage());
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static TaskStatus.Builder newBuilder() {
        return new TaskStatus.Builder();
    }

    public static TaskStatus.Builder newBuilder(TaskStatus taskStatus) {
        return new TaskStatus.Builder(taskStatus);
    }

    public static class Builder extends AbstractBuilder<TaskState, Builder, TaskStatus> {
        private List<ContainerState> containerState;

        private Builder() {
        }

        private Builder(TaskStatus status) {
            super(status);
        }

        public Builder withContainerState(List<ContainerState> containerState) {
            this.containerState = containerState;
            return this;
        }

        @Override
        public TaskStatus build() {
            return new TaskStatus(
                    state,
                    reasonCode == null ? TaskStatus.REASON_UNKNOWN : reasonCode,
                    toCompleteReasonMessage(),
                    timestamp,
                    containerState == CollectionsExt.nonNull(containerState) ? new ArrayList<>() : containerState
            );
        }
    }
}
