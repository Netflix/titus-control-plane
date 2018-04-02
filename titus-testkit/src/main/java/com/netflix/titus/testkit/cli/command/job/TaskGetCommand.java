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

package com.netflix.titus.testkit.cli.command.job;

import java.util.concurrent.CountDownLatch;

import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.cli.command.ErrorReports;
import com.netflix.titus.testkit.util.PrettyPrinters;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.cli.command.ErrorReports;
import com.netflix.titus.testkit.rx.RxGrpcJobManagementService;
import com.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskGetCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(TaskGetCommand.class);

    @Override
    public String getDescription() {
        return "find task by id";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("task_id").hasArg().required()
                .desc("Task id").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();
        String id = cli.getOptionValue('i');

        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcJobManagementService(context.createChannel())
                .findTask(TaskId.newBuilder().setId(id).build())
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        result -> logger.info("Found task: " + PrettyPrinters.print(result)),
                        e -> ErrorReports.handleReplyError("Command execution error", e)
                );
        latch.await();
    }
}
