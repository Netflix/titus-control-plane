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

import com.netflix.titus.testkit.util.PrettyPrinters;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.rx.RxGrpcAgentManagementService;
import com.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentObserveCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(AgentObserveCommand.class);

    @Override
    public String getDescription() {
        return "Observe agent state changes";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcAgentManagementService(context.createChannel())
                .observeAgents()
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        result -> logger.info("Agent event: " + PrettyPrinters.print(result)),
                        e -> logger.error("Command execution error", e)
                );
        latch.await();
    }
}