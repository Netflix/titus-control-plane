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

package com.netflix.titus.api.jobmanager.store;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.common.framework.fit.AbstractFitAction;
import com.netflix.titus.common.framework.fit.FitActionDescriptor;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitUtil;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

public class JobStoreFitAction extends AbstractFitAction {

    public enum ErrorKind {
        /**
         * Drop job ids when loading from the database. Jobs with dropped ids will not be loaded.
         */
        LostJobIds,

        /**
         * Create phantom job ids, and inject them when loading the data from the database.
         */
        PhantomJobIds,

        /**
         * Drop task ids when loading from the database. Tasks with dropped ids will not be loaded.
         */
        LostTaskIds,

        /**
         * Create phantom tasks ids, and inject them when loading the data from the database.
         */
        PhantomTaskIds,

        /**
         * Change {@link Job} instances such that they fail the validation rules.
         */
        CorruptedJobRecords,

        /**
         * Change serialized job data, so it cannot be parsed, and mapped into {@link Job} object.
         */
        CorruptedRawJobRecords,

        /**
         * Change {@link Task} instances such that they fail the validation rules.
         */
        CorruptedTaskRecords,

        /**
         * Change serialized task data, so it cannot be parsed, and mapped into {@link Task} object.
         */
        CorruptedRawTaskRecords,

        /**
         * Duplicate ENI assignments by copying them between tasks which are loaded from the database.
         */
        DuplicatedEni,

        /**
         * Change/remove Mesos assigned resources (IP, ENI) for a launched task.
         */
        CorruptedTaskPlacementData
    }

    private final Set<ErrorKind> failurePoints;
    private final Supplier<Boolean> shouldFailFunction;

    public static final FitActionDescriptor DESCRIPTOR = new FitActionDescriptor(
            "jobStore",
            "Inject failures in JobStore",
            CollectionsExt.copyAndAdd(
                    FitUtil.PERIOD_ERROR_PROPERTIES,
                    "errorKinds", "A list of: " + StringExt.concatenate(ErrorKind.values(), ",")
            )
    );

    private final ConcurrentMap<String, ConcurrentMap<Integer, TwoLevelResource>> twoLevelResourceAssignments = new ConcurrentHashMap<>();

    public JobStoreFitAction(String id, Map<String, String> properties, FitInjection injection) {
        super(id, DESCRIPTOR, properties, injection);

        String errorKindsValue = properties.get("errorKinds");
        Preconditions.checkArgument(errorKindsValue != null, "Missing 'errorKinds' property");

        this.failurePoints = new HashSet<>(StringExt.parseEnumListIgnoreCase(errorKindsValue, ErrorKind.class));
        this.shouldFailFunction = FitUtil.periodicErrors(properties);
    }

    @Override
    public <T> T afterImmediate(String injectionPoint, T result) {
        if (result == null) {
            return null;
        }

        ErrorKind errorKind = ErrorKind.valueOf(injectionPoint);

        if (shouldFailFunction.get() && failurePoints.contains(errorKind)) {
            switch (errorKind) {
                case LostJobIds:
                case LostTaskIds:
                    return handleLostIds(result);
                case PhantomJobIds:
                case PhantomTaskIds:
                    return handlePhantomIds(result);
                case CorruptedJobRecords:
                    return handleCorruptedJobRecords(result);
                case CorruptedTaskRecords:
                    return handleCorruptedTaskRecords(result);
                case CorruptedRawJobRecords:
                case CorruptedRawTaskRecords:
                    return handleCorruptedRawData(result);
                case DuplicatedEni:
                    return handleDuplicatedEni(result, false);
                case CorruptedTaskPlacementData:
                    return handleCorruptedTaskPlacementData(result);
            }
            return result;
        }

        // Do nothing
        switch (errorKind) {
            case LostJobIds:
            case LostTaskIds:
            case CorruptedJobRecords:
            case CorruptedRawJobRecords:
            case CorruptedTaskRecords:
            case CorruptedRawTaskRecords:
            case CorruptedTaskPlacementData:
            case PhantomJobIds:
            case PhantomTaskIds:
                return result;
            case DuplicatedEni:
                return handleDuplicatedEni(result, true);
        }
        return result;
    }

    private <T> T handleLostIds(T result) {
        return null;
    }

    private <T> T handlePhantomIds(T result) {
        return (T) UUID.randomUUID().toString();
    }

    private <T> T handleCorruptedJobRecords(T result) {
        if (result instanceof Job) {
            JobDescriptor changedJobDescriptor = ((Job<?>) result).getJobDescriptor().toBuilder().withContainer(null).build();
            return (T) (Job<?>) ((Job<?>) result).toBuilder().withJobDescriptor(changedJobDescriptor).build();
        }
        return result;
    }

    private <T> T handleCorruptedTaskRecords(T result) {
        if (result instanceof Task) {
            Task changedTask = ((Task) result).toBuilder().withStatus(null).build();
            return (T) changedTask;
        }
        return result;
    }

    private <T> T handleCorruptedRawData(T result) {
        return result instanceof String ? (T) ("BadJSON" + result) : result;
    }

    private <T> T handleDuplicatedEni(T result, boolean storeOnly) {
        if (!(result instanceof Task)) {
            return result;
        }

        Task task = (Task) result;
        if (task.getTwoLevelResources().isEmpty()) {
            return result;
        }
        TwoLevelResource original = task.getTwoLevelResources().get(0);

        synchronized (twoLevelResourceAssignments) {
            // Store current assignment
            ConcurrentMap<Integer, TwoLevelResource> agentAssignments = twoLevelResourceAssignments.computeIfAbsent(
                    task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, "DEFAULT"),
                    k -> new ConcurrentHashMap<>()
            );
            agentAssignments.put(original.getIndex(), original);

            if (storeOnly) {
                return result;
            }

            // Find another assignment on the same agent with different resource value
            Optional<Task> taskOverride = agentAssignments.values().stream().filter(a -> !a.getValue().equals(original.getValue()))
                    .findFirst()
                    .map(match -> {
                        TwoLevelResource override = original.toBuilder().withIndex(match.getIndex()).build();
                        return task.toBuilder().withTwoLevelResources(override).build();
                    });
            return (T) taskOverride.orElse(task);
        }
    }

    private <T> T handleCorruptedTaskPlacementData(T result) {
        if (!(result instanceof Task)) {
            return result;
        }

        Task task = (Task) result;

        Map<String, String> corruptedTaskContext = CollectionsExt.copyAndRemove(
                task.getTaskContext(),
                TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST
        );
        return (T) task.toBuilder().withTaskContext(corruptedTaskContext).build();
    }
}
