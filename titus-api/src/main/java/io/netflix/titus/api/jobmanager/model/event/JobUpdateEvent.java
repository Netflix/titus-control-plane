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

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;

/**
 */
public class JobUpdateEvent extends JobEvent {

    private final Optional<Job> job;
    private final Optional<Job> previousJobVersion;
    private final ModelActionHolder.Model model;

    public JobUpdateEvent(EventType eventType, ModelActionHolder actionHolder, Optional<Job> job, Optional<Job> previousJobVersion, Optional<Throwable> error) {
        super(eventType,actionHolder, error);
        this.job = job;
        this.model = actionHolder.getModel();
        this.previousJobVersion = previousJobVersion;
    }

    public Optional<Job> getJob() {
        return job;
    }

    public Optional<Job> getPreviousJobVersion() {
        return previousJobVersion;
    }

    public ModelActionHolder.Model getModel() {
        return model;
    }

    public String toLogString() {
        return String.format("%-12s %s %-30s %-10s %-29s %-19s %-20s %s",
                "target=Job,",
                "id=" + getId() + ',',
                "eventType=" + getEventType() + ',',
                "status=" + getError().map(e -> "ERROR").orElse("OK") + ',',
                "action=" + getActionName() + ',',
                "trigger=" + getTrigger() + ',',
                "model=" + getModel() + ',',
                "summary=" + getError().map(e -> "ERROR: " + getSummary() + '(' + e.getMessage() + ')').orElse(getSummary())
        );
    }
}
