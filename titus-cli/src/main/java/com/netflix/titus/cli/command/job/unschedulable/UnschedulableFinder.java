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

package com.netflix.titus.cli.command.job.unschedulable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.cli.CommandContext;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;

public class UnschedulableFinder {

    static Map<String, UnschedulableJob> findUnschedulableJobs(CommandContext context,
                                                               Map<String, Job> jobs,
                                                               Map<String, Map<String, Task>> tasks,
                                                               long stuckInAcceptedThresholdMs) {
        Map<String, UnschedulableJob> suspectedJobs = new HashMap<>();
        tasks.forEach((jobId, jobTasks) ->
                processJob(context, jobs.get(jobId), jobTasks, stuckInAcceptedThresholdMs).ifPresent(u -> suspectedJobs.put(jobId, u))
        );
        return suspectedJobs;
    }

    private static Optional<UnschedulableJob> processJob(CommandContext context, Job job, Map<String, Task> tasks, long stuckInAcceptedThresholdMs) {
        if (tasks.isEmpty()) {
            return Optional.empty();
        }

        boolean anyScheduled = tasks.values().stream().anyMatch(t -> t.getStatus().getState() != TaskState.Accepted);
        if (anyScheduled) {
            return Optional.empty();
        }

        // All tasks not scheduled yet. Check if all of them are in the Accepted state long enough to be regarded
        // as not-schedulable.
        long youngest = Long.MAX_VALUE;
        long oldest = Long.MIN_VALUE;
        for (Task task : tasks.values()) {
            long acceptedTimestamp = task.getStatus().getTimestamp();
            youngest = Math.min(youngest, acceptedTimestamp);
            oldest = Math.max(oldest, acceptedTimestamp);
            long waitTimeMs = System.currentTimeMillis() - acceptedTimestamp;
            if (waitTimeMs < stuckInAcceptedThresholdMs) {
                return Optional.empty();
            }
        }

        // Fetch scheduling result for any task.
        SchedulingResultEvent result = context.getSchedulerServiceBlockingStub().getSchedulingResult(SchedulingResultRequest.newBuilder()
                .setTaskId(CollectionsExt.first(tasks.values()).getId())
                .build()
        );

        // All tasks are in the Accepted state for more than stuckInAcceptedThreshold.
        long now = System.currentTimeMillis();
        String failures;
        try {
            failures = JsonFormat.printer().omittingInsignificantWhitespace().print(result);
        } catch (InvalidProtocolBufferException e) {
            failures = result.toString();
        }
        return Optional.of(new UnschedulableJob(
                job.getId(),
                String.format(
                        "All tasks are stuck in the 'Accepted' state for too long (between %s and %s). " +
                                "Most likely they do not fit into any available node resources.",
                        DateTimeExt.toTimeUnitString(now - oldest, 2),
                        DateTimeExt.toTimeUnitString(now - youngest, 2)
                ),
                failures
        ));
    }
}
