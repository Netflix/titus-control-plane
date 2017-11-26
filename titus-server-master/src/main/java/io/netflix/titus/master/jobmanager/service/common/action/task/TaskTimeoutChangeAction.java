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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

/**
 * Associates a timeout with transient task states (Launched|StartInitiated). If task does not move out of this state
 * within an expected amount of time, it is killed.
 * <p>
 * <h1>Why cannot use task state timestamp?</h1>
 * Due to TitusMaster failover process, and delayed Mesos state reconciliation. If we would do this, we might
 * kill tasks that progressed their state, but we are not aware of this, due to a delay in the reconciliation process.
 * This could be solved by doing Mesos reconciliation during system bootstrap, before job reconciliation process starts
 * (a possible improvement in the future).
 */
public class TaskTimeoutChangeAction extends TitusChangeAction {

    public enum TimeoutStatus {Ignore, NotSet, Pending, TimedOut}

    private static final Map<TaskState, String> STATE_TAGS = ImmutableMap.of(
            TaskState.Launched, "interceptor.timeout.launched",
            TaskState.StartInitiated, "interceptor.timeout.startInitiated",
            TaskState.KillInitiated, "interceptor.timeout.killInitiated"
    );

    private final String tagName;
    private final long deadlineMs;

    public TaskTimeoutChangeAction(String id, TaskState taskState, long deadlineMs) {
        super(new JobChange(ActionKind.Task, Trigger.Reconciler, id, "Setting timeout for task " + id));

        this.tagName = STATE_TAGS.get(taskState);
        Preconditions.checkArgument(tagName != null, "Timeout not tracked for state %s", taskState);

        this.deadlineMs = deadlineMs;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
        return Observable.fromCallable(() -> {
            EntityUpdateAction action = new EntityUpdateAction(
                    getChange().getId(),
                    eh -> eh.addTag(tagName, deadlineMs),
                    getChange().getSummary()
            );
            return Pair.of(getChange(), Collections.singletonList(ModelActionHolder.running(action)));
        });
    }

    public static TimeoutStatus getTimeoutStatus(EntityHolder taskHolder, Clock clock) {
        Task task = taskHolder.getEntity();
        TaskState state = task.getStatus().getState();
        if (state != TaskState.Launched && state != TaskState.StartInitiated && state != TaskState.KillInitiated) {
            return TimeoutStatus.Ignore;
        }

        Long deadline = (Long) taskHolder.getAttributes().get(STATE_TAGS.get(state));
        if (deadline == null) {
            return TimeoutStatus.NotSet;
        }
        return clock.wallTime() < deadline ? TimeoutStatus.Pending : TimeoutStatus.TimedOut;
    }

    static class EntityUpdateAction extends TitusModelUpdateAction {

        private final Function<EntityHolder, EntityHolder> updateFun;

        EntityUpdateAction(String id, Function<EntityHolder, EntityHolder> updateFun, String summary) {
            super(ActionKind.Task, Trigger.Reconciler, id, summary);
            this.updateFun = updateFun;
        }

        @Override
        public Pair<EntityHolder, Optional<EntityHolder>> apply(EntityHolder rootHolder) {
            return rootHolder.findById(getId()).map(eh -> {
                EntityHolder newChild = updateFun.apply(eh);
                return Pair.of(rootHolder.addChild(newChild), Optional.of(newChild));
            }).orElseGet(() -> Pair.of(rootHolder, Optional.empty()));
        }
    }
}
