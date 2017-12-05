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

package io.netflix.titus.master.jobmanager.service.common.action;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import rx.Observable;

/**
 */
public abstract class TitusChangeAction implements ChangeAction<JobChange> {

    private final JobChange change;

    protected TitusChangeAction(JobChange change) {
        this.change = change;
    }

    public JobChange getChange() {
        return change;
    }

    public static Builder newAction(String name) {
        return new Builder(name);
    }

    public static Builder newInterceptor(String name, TitusChangeAction changeAction) {
        JobChange jobChange = changeAction.getChange();
        return new Builder(name + '(' + jobChange.getName() + ')')
                .id(jobChange.getId())
                .trigger(jobChange.getTrigger())
                .summary(name + ": " + jobChange.getSummary());
    }

    public static class Builder {

        final String name;
        String id;
        String summary = "None";
        Trigger trigger;

        public Builder(String name) {
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
            JobChange jobChange = newJobChange();
            return new TitusChangeAction(newJobChange()) {
                @Override
                public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
                    return actionFun.apply(Builder.this).map(modelActionHolder -> Pair.of(jobChange, Collections.singletonList(modelActionHolder)));
                }
            };
        }

        public TitusChangeAction changeWithModelUpdates(Function<Builder, Observable<List<ModelActionHolder>>> actionFun) {
            JobChange jobChange = newJobChange();
            return new TitusChangeAction(jobChange) {
                @Override
                public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
                    return actionFun.apply(Builder.this).map(modelActionHolders -> Pair.of(jobChange, modelActionHolders));
                }
            };
        }

        public TitusChangeAction applyModelUpdate(Function<Builder, ModelActionHolder> actionFun) {
            JobChange jobChange = newJobChange();
            return new TitusChangeAction(newJobChange()) {
                @Override
                public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
                    return Observable.fromCallable(() -> actionFun.apply(Builder.this))
                            .map(modelActionHolder -> Pair.of(jobChange, Collections.singletonList(modelActionHolder)));
                }
            };
        }

        public TitusChangeAction applyModelUpdates(Function<Builder, List<ModelActionHolder>> actionFun) {
            JobChange jobChange = newJobChange();
            return new TitusChangeAction(newJobChange()) {
                @Override
                public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
                    return Observable.fromCallable(() -> actionFun.apply(Builder.this))
                            .map(modelActionHolders -> Pair.of(jobChange, modelActionHolders));
                }
            };
        }

        private JobChange newJobChange() {
            Preconditions.checkState(id != null, "Job or task id not defined");
            Preconditions.checkState(trigger != null, "Trigger not defined");

            return new JobChange(trigger, id, name, summary);
        }
    }
}
