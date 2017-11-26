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

package io.netflix.titus.api.jobmanager.service.common.action;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.common.framework.reconciler.ModelAction;

/**
 */
public abstract class TitusModelUpdateAction implements ModelAction {

    private final ActionKind actionKind;
    private final JobManagerEvent.Trigger trigger;
    private final String id;
    private final String summary;

    protected TitusModelUpdateAction(ActionKind actionKind, JobManagerEvent.Trigger trigger, String id, String summary) {
        this.actionKind = actionKind;
        this.trigger = trigger;
        this.id = id;
        this.summary = summary;
    }

    public String getId() {
        return id;
    }

    public ActionKind getActionKind() {
        return actionKind;
    }

    public JobManagerEvent.Trigger getTrigger() {
        return trigger;
    }


    public String getSummary() {
        return summary;
    }
}
