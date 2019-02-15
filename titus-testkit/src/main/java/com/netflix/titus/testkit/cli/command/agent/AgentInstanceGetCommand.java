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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import com.netflix.titus.api.model.Page;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentInstanceGetCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(AgentServerGroupGetCommand.class);

    @Override
    public String getDescription() {
        return "Get agent instance(s)";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("instance_id").hasArg()
                .desc("Agent instance id").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();
        if (!cli.hasOption('i')) {
            getInstances(context);
        } else {
            getInstance(context, cli.getOptionValue('i'));
        }
    }

    private void getInstance(CommandContext context, String id) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        context.getAgentManagementClient()
                .getAgentInstance(id)
                .doOnSubscribe(s -> latch.countDown())
                .subscribe(
                        result -> logger.info("Found agent: {}", result),
                        e -> logger.error("Command execution error", e)
                );
        latch.await();
    }

    private void getInstances(CommandContext context) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        context.getAgentManagementClient()
                .findAgentInstances(Collections.emptyMap(), Page.unlimited())
                .doOnSubscribe(s -> latch.countDown())
                .subscribe(
                        result -> logger.info("Found agent instances: {}", result),
                        e -> logger.error("Command execution error", e)
                );
        latch.await();
    }
}