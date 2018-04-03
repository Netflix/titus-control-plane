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

import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.cli.command.ErrorReports;
import com.netflix.titus.testkit.rx.RxGrpcJobManagementService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobResizeCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(JobResizeCommand.class);

    @Override
    public String getDescription() {
        return "change job size (min/desired/max)";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("job_id").hasArg().required()
                .desc("Job id").build());
        options.addOption(Option.builder("min").longOpt("min").hasArg(true).type(Number.class).required()
                .desc("Job min size").build());
        options.addOption(Option.builder("desired").longOpt("desired").hasArg(true).type(Number.class).required()
                .desc("Job min size").build());
        options.addOption(Option.builder("max").longOpt("max").hasArg(true).type(Number.class).required()
                .desc("Job min size").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();
        String id = cli.getOptionValue('i');
        int min = (int) (long) cli.getParsedOptionValue("min");
        int desired = (int) (long) cli.getParsedOptionValue("desired");
        int max = (int) (long) cli.getParsedOptionValue("max");

        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcJobManagementService(context.createChannel())
                .updateJobSize(id, min, desired, max)
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        never -> {
                        },
                        e -> ErrorReports.handleReplyError("Command execution error", e),
                        () -> logger.info("Job size changed")
                );
        latch.await();
    }
}
