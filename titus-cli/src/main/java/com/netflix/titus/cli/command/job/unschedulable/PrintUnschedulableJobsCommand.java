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

import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.cli.CliCommand;
import com.netflix.titus.cli.CommandContext;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.unit.TimeUnitExt;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.cli.command.job.JobUtil.loadActiveJobsAndTasks;

public class PrintUnschedulableJobsCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(PrintUnschedulableJobsCommand.class);

    @Override
    public String getDescription() {
        return "print all unschedulable jobs";
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
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        long stuckInAcceptedThresholdMs = TimeUnitExt.toMillis(context.getCLI().getOptionValue('e'))
                .orElseThrow(() -> new IllegalArgumentException("Wrong expiry threshold"));

        Pair<Map<String, Job>, Map<String, Map<String, Task>>> all = loadActiveJobsAndTasks(context);
        Map<String, UnschedulableJob> unschedulable = UnschedulableFinder.findUnschedulableJobs(all.getLeft(), all.getRight(), stuckInAcceptedThresholdMs);
        logger.info("Found {} unschedulable jobs", unschedulable.size());
        unschedulable.forEach((jobId, info) -> logger.info("    {}: {}", jobId, info.getReason()));
    }
}