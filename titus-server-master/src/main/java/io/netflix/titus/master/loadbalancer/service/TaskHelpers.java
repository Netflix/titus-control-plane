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

package io.netflix.titus.master.loadbalancer.service;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TaskHelpers {
    private static final Logger logger = LoggerFactory.getLogger(TaskHelpers.class);

    static boolean isStateTransition(TaskUpdateEvent event) {
        final Task currentTask = event.getCurrentTask();
        final Optional<Task> previousTask = event.getPreviousTask();
        boolean identical = previousTask.map(previous -> previous == currentTask).orElse(false);
        return !identical && previousTask
                .map(previous -> !previous.getStatus().getState().equals(currentTask.getStatus().getState()))
                .orElse(false);
    }

    static boolean isStartedWithIp(Task task) {
        return hasIpAndStateMatches(task, TaskState.Started::equals);
    }

    static boolean isTerminalWithIp(Task task) {
        return hasIpAndStateMatches(task, state -> {
            switch (task.getStatus().getState()) {
                case KillInitiated:
                case Finished:
                case Disconnected:
                    return true;
                default:
                    return false;
            }
        });
    }

    private static boolean hasIpAndStateMatches(Task task, Function<TaskState, Boolean> predicate) {
        final TaskState state = task.getStatus().getState();
        if (!predicate.apply(state)) {
            return false;
        }
        final boolean hasIp = task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
        if (!hasIp) {
            logger.warn("Task {} has state {} but no ipAddress associated", task.getId(), state);
        }
        return hasIp;

    }

    static Set<String> ipAddresses(Collection<TargetStateBatchable> from) {
        return from.stream().map(TargetStateBatchable::getIpAddress).collect(Collectors.toSet());
    }
}
