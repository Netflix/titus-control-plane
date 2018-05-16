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

package com.netflix.titus.testkit.cli.command.scheduler;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ObserveSchedulingResultCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(ObserveSchedulingResultCommand.class);

    @Override
    public String getDescription() {
        return "observe scheduler result for a task";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("task_id").hasArg().desc("Task id").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        String taskId = context.getCLI().getOptionValue('i');
        SchedulerServiceGrpc.newBlockingStub(context.createChannel()).observeSchedulingResults(
                SchedulingResultRequest.newBuilder().setTaskId(taskId).build()
        ).forEachRemaining(event -> {
            switch (event.getStatusCase()) {
                case SUCCESS:
                    logger.info("Task scheduled: {}", event.getSuccess().getMessage());
                    break;
                case FAILURES:
                    List<String> failures = event.getFailures().getFailuresList().stream()
                            .map(f -> String.format("%s (failureCount=%d): %s", f.getReason(), f.getFailureCount(), f.getAgentIdSamplesList()))
                            .collect(Collectors.toList());
                    logger.info("Failures: {}", failures);
                    break;
                default:
                    logger.warn("Unexpected event: {}", event);
            }
        });
    }
}
