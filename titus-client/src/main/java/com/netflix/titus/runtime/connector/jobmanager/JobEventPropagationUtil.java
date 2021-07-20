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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.event.EventPropagationTrace;
import com.netflix.titus.common.util.event.EventPropagationUtil;
import com.netflix.titus.common.util.time.Clock;

/**
 * Help utils for measuring job event notification propagation delays.
 */
public class JobEventPropagationUtil {

    public static final String CHECKPOINT_TJC_INTERNAL = "tjcInternal";
    public static final String CHECKPOINT_GATEWAY_CLIENT = "gwClient";
    public static final String CHECKPOINT_GATEWAY_INTERNAL = "gwInternal";
    public static final String CHECKPOINT_FED_CLIENT = "fedClient";
    public static final String CHECKPOINT_FED_INTERNAL = "federationInternal";

    public static final List<String> FEDERATION_LABELS = Arrays.asList(
            CHECKPOINT_TJC_INTERNAL,
            CHECKPOINT_GATEWAY_CLIENT,
            CHECKPOINT_GATEWAY_INTERNAL,
            CHECKPOINT_FED_CLIENT,
            CHECKPOINT_FED_INTERNAL
    );

    public static <E extends JobDescriptorExt> Job<E> recordChannelLatency(String id, Job<E> job, long timestamp, Clock clock) {
        Map<String, String> updated = EventPropagationUtil.copyAndAddNextStage(id, job.getJobDescriptor().getAttributes(), timestamp, clock.wallTime());
        return job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder().withAttributes(updated).build())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.Job recordChannelLatency(String id, com.netflix.titus.grpc.protogen.Job job, long timestamp, Clock clock) {
        String next = EventPropagationUtil.buildNextStage(id, job.getJobDescriptor().getAttributesMap(), timestamp, clock.wallTime());
        return job.toBuilder()
                .setJobDescriptor(job.getJobDescriptor().toBuilder()
                        .putAttributes(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES, next)
                        .build()
                )
                .build();
    }

    public static Task recordChannelLatency(String id, Task task, long timestamp, Clock clock) {
        Map<String, String> updated = EventPropagationUtil.copyAndAddNextStage(id, task.getAttributes(), timestamp, clock.wallTime());
        return task.toBuilder().withAttributes(updated).build();
    }

    public static com.netflix.titus.grpc.protogen.Task recordChannelLatency(String id, com.netflix.titus.grpc.protogen.Task task, long timestamp, Clock clock) {
        String next = EventPropagationUtil.buildNextStage(id, task.getAttributesMap(), timestamp, clock.wallTime());
        return task.toBuilder()
                .putAttributes(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES, next)
                .build();
    }

    public static Optional<EventPropagationTrace> newTrace(Job job, boolean snapshot, List<String> labels) {
        return EventPropagationUtil.parseTrace(job.getJobDescriptor().getAttributes(), snapshot, job.getVersion().getTimestamp(), labels);
    }
}
