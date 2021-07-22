/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.event.EventPropagationMetrics;
import com.netflix.titus.common.util.event.EventPropagationTrace;
import com.netflix.titus.common.util.event.EventPropagationUtil;

import static com.netflix.titus.runtime.connector.jobmanager.JobEventPropagationUtil.FEDERATION_LABELS;

public class JobEventPropagationMetrics {

    private final List<String> stageNames;

    private final EventPropagationMetrics metrics;

    private JobEventPropagationMetrics(String name, List<String> stageNames, TitusRuntime titusRuntime) {
        this.stageNames = stageNames;
        this.metrics = new EventPropagationMetrics(
                titusRuntime.getRegistry().createId(name),
                stageNames,
                titusRuntime
        );
    }

    public Optional<EventPropagationTrace> recordJob(Job job, boolean snapshot) {
        Optional<EventPropagationTrace> trace = EventPropagationUtil.parseTrace(
                job.getJobDescriptor().getAttributes(),
                snapshot,
                job.getVersion().getTimestamp(),
                stageNames
        );
        trace.ifPresent(metrics::record);
        return trace;
    }

    public Optional<EventPropagationTrace> recordTask(Task task, boolean snapshot) {
        Optional<EventPropagationTrace> trace = EventPropagationUtil.parseTrace(
                task.getAttributes(),
                snapshot,
                task.getVersion().getTimestamp(),
                stageNames
        );
        trace.ifPresent(metrics::record);
        return trace;
    }

    public static JobEventPropagationMetrics newFederationMetrics(TitusRuntime titusRuntime) {
        return new JobEventPropagationMetrics("titus.eventPropagation", FEDERATION_LABELS, titusRuntime);
    }

    public static JobEventPropagationMetrics newExternalClientMetrics(String clientName, TitusRuntime titusRuntime) {
        List<String> labels = CollectionsExt.copyAndAdd(FEDERATION_LABELS, clientName);
        return new JobEventPropagationMetrics("titus.eventPropagation", labels, titusRuntime);
    }
}
