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

import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;

public class JobEvent extends JobManagerEvent {

    public JobEvent(EventType eventType, TitusChangeAction changeAction, Optional<Throwable> error) {
        super(eventType, changeAction.getClass().getSimpleName(), changeAction.getChange().getId(), changeAction.getChange().getTrigger(), changeAction.getChange().getSummary(), error);
    }

    public JobEvent(EventType eventType, ModelActionHolder actionHolder, Optional<Throwable> error) {
        super(eventType,
                "jobUpdate",
                ((TitusModelUpdateAction) actionHolder.getAction()).getId(),
                ((TitusModelUpdateAction) actionHolder.getAction()).getTrigger(),
                ((TitusModelUpdateAction) actionHolder.getAction()).getSummary(),
                error
        );
    }
}
