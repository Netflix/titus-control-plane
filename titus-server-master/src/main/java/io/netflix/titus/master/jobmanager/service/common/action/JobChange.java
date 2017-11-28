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

/**
 * Describes a job or task change action.
 */
public class JobChange {

    public enum Trigger {
        API,
        Mesos,
        Reconciler
    }

    private final Trigger trigger;
    private final String id;
    private final String summary;

    public JobChange(Trigger trigger, String id, String summary) {
        this.trigger = trigger;
        this.id = id;
        this.summary = summary;
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
}
