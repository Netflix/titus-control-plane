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

import io.netflix.titus.common.framework.reconciler.ReconcilerEvent;

/**
 */
public abstract class JobManagerEvent implements ReconcilerEvent {

    public enum Trigger {
        API,
        Mesos,
        Reconciler
    }

    private final EventType eventType;
    private final String actionName;
    private final String id;
    private final Trigger trigger;
    private final String summary;
    private final Optional<Throwable> error;

    protected JobManagerEvent(EventType eventType, String actionName, String id, Trigger trigger, String summary, Optional<Throwable> error) {
        this.eventType = eventType;
        this.actionName = actionName;
        this.id = id;
        this.trigger = trigger;
        this.summary = summary;
        this.error = error;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getActionName() {
        return actionName;
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

    public Optional<Throwable> getError() {
        return error;
    }

    public String toLogString() {
        return String.format("%-12s %s %-30s %-10s %-29s %-19s %s",
                "target=" + (getClass().getSimpleName().contains("Job") ? "Job" : "Task") + ',',
                "id=" + getId() + ',',
                "eventType=" + getEventType() + ',',
                "status=" + getError().map(e -> "ERROR").orElse("OK") + ',',
                "action=" + getActionName() + ',',
                "trigger=" + getTrigger() + ',',
                "summary=" + getError().map(e -> "ERROR: " + getSummary() + '(' + e.getMessage() + ')').orElse(getSummary())
        );
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "eventType=" + eventType +
                ", actionName='" + actionName + '\'' +
                ", id='" + id + '\'' +
                ", trigger=" + trigger +
                ", summary='" + summary + '\'' +
                ", error=" + error +
                '}';
    }
}
