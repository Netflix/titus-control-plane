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

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import rx.Observable;

/**
 */
public abstract class TitusChangeAction implements ChangeAction {

    private final V3JobOperations.Trigger trigger;
    private final String id;
    private final String name;
    private final String summary;

    protected TitusChangeAction(TitusChangeAction delegate) {
        this.trigger = delegate.getTrigger();
        this.id = delegate.getId();
        this.name = delegate.getName();
        this.summary = delegate.getSummary();
    }

    public TitusChangeAction(Trigger trigger, String id, String name, String summary) {
        this.trigger = trigger;
        this.id = id;
        this.name = name;
        this.summary = summary;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getSummary() {
        return summary;
    }

    public static Builder newAction(String name) {
        return new Builder(name);
    }

    public static Builder newInterceptor(String name, TitusChangeAction changeAction) {
        return new Builder(name + '(' + changeAction.getName() + ')')
                .id(changeAction.getId())
                .trigger(changeAction.getTrigger())
                .summary(name + ": " + changeAction.getSummary());
    }

    public static class Builder {

        final String name;
        String id;
        String summary = "None";
        Trigger trigger;

        private Builder(String name) {
            this.name = name;
        }

        public Builder id(String id) {
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

        public TitusChangeAction changeWithModelUpdate(Function<Builder, Observable<ModelActionHolder>> actionFun) {
            check();
            return new TitusChangeAction(trigger, id, name, summary) {
                @Override
                public Observable<List<ModelActionHolder>> apply() {
                    return actionFun.apply(Builder.this).map(Collections::singletonList);
                }
            };
        }

        public TitusChangeAction changeWithModelUpdates(Function<Builder, Observable<List<ModelActionHolder>>> actionFun) {
            check();
            return new TitusChangeAction(trigger, id, name, summary) {
                @Override
                public Observable<List<ModelActionHolder>> apply() {
                    return actionFun.apply(Builder.this);
                }
            };
        }

        public TitusChangeAction applyModelUpdate(Function<Builder, ModelActionHolder> actionFun) {
            check();
            return new TitusChangeAction(trigger, id, name, summary) {
                @Override
                public Observable<List<ModelActionHolder>> apply() {
                    return Observable.fromCallable(() -> actionFun.apply(Builder.this)).map(Collections::singletonList);
                }
            };
        }

        public TitusChangeAction applyModelUpdates(Function<Builder, List<ModelActionHolder>> actionFun) {
            check();
            return new TitusChangeAction(trigger, id, name, summary) {
                @Override
                public Observable<List<ModelActionHolder>> apply() {
                    return Observable.fromCallable(() -> actionFun.apply(Builder.this));
                }
            };
        }

        private void check() {
            Preconditions.checkState(id != null, "Job or task id not defined");
            Preconditions.checkState(trigger != null, "Trigger not defined");
        }
    }
}
