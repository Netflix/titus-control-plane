/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.cli.command.job;

import java.time.Duration;

import com.netflix.titus.cli.CliCommand;
import com.netflix.titus.cli.CommandContext;
import com.netflix.titus.cli.command.ErrorReports;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillTaskCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(KillTaskCommand.class);

    @Override
    public String getDescription() {
        return "Kill a task";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("task_id").hasArg().required().desc("Task id").build());
        options.addOption(Option.builder("s").longOpt("shrink").desc("Shrink job").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();
        String id = cli.getOptionValue('i');
        boolean shrink = cli.hasOption('s');

        try {
            context.getJobManagementClient().killTask(id, shrink, context.getCallMetadata("Kill task command"))
                    .block(Duration.ofMinutes(1));
            logger.info("Killed task {}", id);
        } catch (Exception e) {
            ErrorReports.handleReplyError("Command execution error", e);
        }
    }
}
