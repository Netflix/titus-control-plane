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

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.cli.CliCommand;
import com.netflix.titus.cli.CommandContext;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.unit.TimeUnitExt;
import com.netflix.titus.runtime.connector.jobmanager.RemoteJobManagementClient;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.cli.command.job.JobUtil.loadActiveJobsAndTasks;

public class RemoveUnschedulableJobsCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(PrintUnschedulableJobsCommand.class);

    @Override
    public String getDescription() {
        return "remove unschedulable jobs";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("e").longOpt("expiry").hasArg().required()
                .desc("Duration threshold after which a not scheduled task can be regarded as non-schedulable (8h, 5d, etc).").build());
        options.addOption(Option.builder("l").longOpt("limit").hasArg().required()
                .desc("Maximum number of jobs to remove").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        long stuckInAcceptedThresholdMs = TimeUnitExt.toMillis(context.getCLI().getOptionValue('e'))
                .orElseThrow(() -> new IllegalArgumentException("Wrong expiry threshold"));
        int limit = Integer.parseInt(context.getCLI().getOptionValue('l'));

        Pair<Map<String, Job>, Map<String, Map<String, Task>>> all = loadActiveJobsAndTasks(context);
        Map<String, Job> jobs = all.getLeft();

        Map<String, UnschedulableJob> unschedulable = UnschedulableFinder.findUnschedulableJobs(context, all.getLeft(), all.getRight(), stuckInAcceptedThresholdMs);
        logger.info("Found {} unschedulable jobs", unschedulable.size());
        logger.info("Removing the oldest {}...", limit);

        List<Job> orderedJobs = unschedulable.keySet().stream().map(jobs::get)
                .sorted(Comparator.comparingLong(j -> j.getStatus().getTimestamp()))
                .collect(Collectors.toList());

        RemoteJobManagementClient jobClient = context.getJobManagementClient();
        int len = Math.min(orderedJobs.size(), limit);
        for (int i = 0; i < len; i++) {
            Job jobToRemove = orderedJobs.get(i);
            logger.info("Removing job {}...", jobToRemove);
            CallMetadata callMetadata = CallMetadata.newBuilder()
                    .withCallReason(unschedulable.get(jobToRemove.getId()).getReason())
                    .withCallers(Collections.singletonList(
                            Caller.newBuilder()
                                    .withId(System.getenv("USER"))
                                    .withCallerType(CallerType.User)
                                    .build()
                    ))
                    .build();
            jobClient.killJob(jobToRemove.getId(), callMetadata).block(Duration.ofSeconds(60));
        }
        logger.info("Removed {} unschedulable jobs out of {} (left {})", len, unschedulable.size(), unschedulable.size() - len);
    }
}
