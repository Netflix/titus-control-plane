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

package com.netflix.titus.testkit.cli.command.agent;

import java.util.concurrent.CountDownLatch;

import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.rx.RxGrpcAgentManagementService;
import com.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentServerGroupGetCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(AgentServerGroupGetCommand.class);

    @Override
    public String getDescription() {
        return "Get agent server group(s)";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("server_group_id").hasArg()
                .desc("Agent server group id").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();
        if (!cli.hasOption('i')) {
            getServerGroups(context);
        } else {
            getServerGroup(context, cli.getOptionValue('i'));
        }
    }

    private void getServerGroup(CommandContext context, String id) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcAgentManagementService(context.createChannel())
                .getInstanceGroup(id)
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        result -> logger.info("Found agent server group: " + PrettyPrinters.print(result)),
                        e -> logger.error("Command execution error", e)
                );
        latch.await();
    }

    private void getServerGroups(CommandContext context) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcAgentManagementService(context.createChannel())
                .getInstanceGroups()
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        result -> logger.info("Found agent server group: " + PrettyPrinters.print(result)),
                        e -> logger.error("Command execution error", e)
                );
        latch.await();
    }
}