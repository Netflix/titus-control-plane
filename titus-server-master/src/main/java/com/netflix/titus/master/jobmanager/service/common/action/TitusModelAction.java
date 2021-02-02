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

import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelAction;
import com.netflix.titus.common.util.tuple.Pair;

/**
 */
public abstract class TitusModelAction implements ModelAction {

    private static final CallMetadata NOT_SET_CALLMETADATA = JobManagerConstants.UNDEFINED_CALL_METADATA.toBuilder()
            .withCallReason("WARNING: caller id not set for the model change action")
            .build();

    private final String name;
    private final Trigger trigger;
    private final String id;
    private final String summary;
    private final CallMetadata callMetadata;

    protected TitusModelAction(Trigger trigger, String id, String summary, CallMetadata callMetadata) {
        this.trigger = trigger;
        this.id = id;
        this.summary = summary;
        this.callMetadata = callMetadata == null ? NOT_SET_CALLMETADATA : callMetadata;
        this.name = getClass().getSimpleName();
    }

    protected TitusModelAction(String name, Trigger trigger, String id, String summary, CallMetadata callMetadata) {
        this.name = name;
        this.trigger = trigger;
        this.id = id;
        this.summary = summary;
        this.callMetadata = callMetadata == null ? NOT_SET_CALLMETADATA : callMetadata;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public String getSummary() {
        return summary;
    }

    public CallMetadata getCallMetadata() {
        return callMetadata;
    }

    public static Builder newModelUpdate(String name) {
        return new Builder(name);
    }

    public static Builder newModelUpdate(TitusChangeAction.Builder sourceChangeAction) {
        return new Builder(sourceChangeAction.name)
                .id(sourceChangeAction.id)
                .trigger(sourceChangeAction.trigger)
                .summary(sourceChangeAction.summary)
                .callMetadata(sourceChangeAction.callMetadata);
    }

    public static Builder newModelUpdate(String name, TitusChangeAction sourceChangeAction) {
        return new Builder(name)
                .id(sourceChangeAction.getId())
                .trigger(sourceChangeAction.getTrigger())
                .summary(sourceChangeAction.getSummary())
                .callMetadata(sourceChangeAction.getCallMetadata());
    }

    public static class Builder {

        private final String name;
        private String id;
        private Trigger trigger;
        private String summary = "None";
        private CallMetadata callMetadata;

        private Builder(String name) {
            this.name = name;
        }

        private Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder job(Job<?> job) {
            this.id = job.getId();
            return this;
        }

        public Builder task(Task task) {
            this.id = task.getId();
            return this;
        }

        public Builder trigger(Trigger trigger) {
            this.trigger = trigger;
            return this;
        }

        public Builder summary(String summary, Object... args) {
            this.summary = args.length > 0 ? String.format(summary, args) : summary;
            return this;
        }

        public Builder callMetadata(CallMetadata callMetadata) {
            this.callMetadata = callMetadata;
            return this;
        }

        public TitusModelAction jobMaybeUpdate(Function<EntityHolder, Optional<EntityHolder>> jobHolderFun) {
            check();
            return new TitusModelAction(name, trigger, id, summary, callMetadata) {
                @Override
                public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
                    Optional<EntityHolder> newRoot = jobHolderFun.apply(rootHolder);
                    return newRoot.map(value -> verify(Pair.of(value, value)));
                }
            };
        }

        public TitusModelAction jobUpdate(Function<EntityHolder, EntityHolder> jobHolderFun) {
            return jobMaybeUpdate(eh -> Optional.of(jobHolderFun.apply(eh)));
        }

        public TitusModelAction taskMaybeUpdate(Function<EntityHolder, Optional<Pair<EntityHolder, EntityHolder>>> taskHolderFun) {
            check();
            return new TitusModelAction(name, trigger, id, summary, callMetadata) {
                @Override
                public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
                    return taskHolderFun.apply(rootHolder).map(pair -> verify(pair));
                }
            };
        }

        public TitusModelAction taskUpdate(Function<EntityHolder, Pair<EntityHolder, EntityHolder>> taskHolderFun) {
            return taskMaybeUpdate(eh -> Optional.of(taskHolderFun.apply(eh)));
        }

        public TitusModelAction taskUpdate(Task newTask) {
            return taskUpdate(jobHolder -> JobEntityHolders.addTask(jobHolder, newTask));
        }

        public TitusModelAction addTaskHolder(EntityHolder taskHolder) {
            return taskUpdate(jobHolder -> Pair.of(jobHolder.addChild(taskHolder), taskHolder));
        }

        public TitusModelAction removeTask(Task task) {
            this.id = task.getId();
            return jobMaybeUpdate(jobHolder -> {
                Pair<EntityHolder, Optional<EntityHolder>> result = jobHolder.removeChild(id);
                return result.getRight().map(removed -> result.getLeft());
            });
        }

        private Pair<EntityHolder, EntityHolder> verify(Pair<EntityHolder, EntityHolder> modelUpdate) {
            Object root = modelUpdate.getLeft().getEntity();
            Preconditions.checkArgument(root instanceof Job, "Root entity not Job instance, but %s", root.getClass());

            if (modelUpdate.getLeft() == modelUpdate.getRight()) {
                return modelUpdate;
            }

            Object child = modelUpdate.getRight().getEntity();
            Preconditions.checkArgument(child instanceof Job || child instanceof Task, "Root entity not Job or Task instance, but %s", root.getClass());

            return modelUpdate;
        }

        private void check() {
            Preconditions.checkState(id != null, "Job or task id not defined");
            Preconditions.checkState(trigger != null, "Trigger not defined");
        }
    }
}
