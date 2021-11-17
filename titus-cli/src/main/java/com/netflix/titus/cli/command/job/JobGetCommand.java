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
import java.util.concurrent.CountDownLatch;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.cli.CliCommand;
import com.netflix.titus.cli.CommandContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobGetCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(JobGetCommand.class);

    @Override
    public String getDescription() {
        return "get for a given id";
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
        options.addOption(Option.builder("m").longOpt("monitor").hasArg(false)
                .desc("If set to true, connect to job update stream").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();

        String id = cli.getOptionValue('i');

        if (!cli.hasOption('m')) {
            getOneJob(context, id);
        } else {
            subscribeToJobUpdateStream(context, id);
        }
    }

    private void getOneJob(CommandContext context, String id) {
        Job<?> job = context.getJobManagementClient().findJob(id).block(Duration.ofSeconds(60));
        logger.info("Found job: {}", job);
    }

    private void subscribeToJobUpdateStream(CommandContext context, String id) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        context.getJobManagementClient().observeJob(id)
                .doFinally(signal -> latch.countDown())
                .subscribe(
                        job -> logger.info("Job notification: {}", job),
                        e -> logger.error("Command execution error", e)
                );
        latch.await();
    }
}