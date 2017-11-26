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

package io.netflix.titus.api.jobmanager.model.event;

import java.util.Optional;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent.ReconcileEventFactory;
import io.netflix.titus.common.util.tuple.Pair;

/**
 */
public class JobEventFactory implements ReconcileEventFactory {
    @Override
    public ReconcilerEvent newChangeEvent(ReconcilerEvent.EventType eventType, ChangeAction action, Optional<Throwable> error) {
        Preconditions.checkArgument(
                action instanceof TitusChangeAction,
                "Unexpected ChangeAction type %s", action.getClass()
        );
        TitusChangeAction titusChangeAction = (TitusChangeAction) action;
        switch (titusChangeAction.getChange().getActionKind()) {
            case Job:
                return new JobEvent(eventType, titusChangeAction, error);
            case Task:
                return new TaskEvent(eventType, titusChangeAction, error);
        }
        throw new IllegalStateException("Unknown action kind " + titusChangeAction.getChange().getActionKind() + " of " + titusChangeAction.getClass().getSimpleName());
    }

    @Override
    public ReconcilerEvent newModelUpdateEvent(ReconcilerEvent.EventType eventType,
                                               ModelActionHolder actionHolder,
                                               Optional<EntityHolder> changedEntityHolder,
                                               Optional<EntityHolder> previousEntityHolder,
                                               Optional<Throwable> error) {
        if (eventType == ReconcilerEvent.EventType.ModelInitial) {
            ModelActionHolder initialActionHolder = ModelActionHolder.reference(new InitialUpdateAction(changedEntityHolder.get()));
            return new JobUpdateEvent(eventType,
                    initialActionHolder,
                    changedEntityHolder.map(EntityHolder::getEntity),
                    Optional.empty(),
                    error
            );
        }

        Preconditions.checkArgument(
                actionHolder.getAction() instanceof TitusModelUpdateAction,
                "Unexpected ModelAction type %s", actionHolder.getClass()
        );
        TitusModelUpdateAction titusAction = (TitusModelUpdateAction) actionHolder.getAction();
        switch (titusAction.getActionKind()) {
            case Job:
                return new JobUpdateEvent(eventType,
                        actionHolder,
                        changedEntityHolder.map(EntityHolder::getEntity),
                        previousEntityHolder.map(EntityHolder::getEntity),
                        error
                );
            case Task:
                return new TaskUpdateEvent(eventType,
                        actionHolder,
                        changedEntityHolder.map(EntityHolder::getEntity),
                        previousEntityHolder.map(EntityHolder::getEntity),
                        error
                );
            case Close:
                return new JobClosedEvent(eventType, actionHolder);
        }
        throw new IllegalStateException("Unknown action kind " + titusAction.getActionKind() + " of " + titusAction.getClass().getSimpleName());
    }


    /**
     * To create an event, we need to have model update action. This action is used solely for the purpose of emitting
     * the very first root, after the engine is created.
     */
    private static class InitialUpdateAction extends TitusModelUpdateAction {

        private final EntityHolder initialRoot;

        protected InitialUpdateAction(EntityHolder initialRoot) {
            super(ActionKind.Job, JobManagerEvent.Trigger.API, initialRoot.getId(), "Job created");
            this.initialRoot = initialRoot;
        }

        @Override
        public Pair<EntityHolder, Optional<EntityHolder>> apply(EntityHolder rootHolder) {
            return Pair.of(initialRoot, Optional.of(initialRoot));
        }
    }
}
