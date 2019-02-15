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

import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentLifecycleUpdateCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(AgentLifecycleUpdateCommand.class);

    @Override
    public String getDescription() {
        return "Change agent server group lifecycle status";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("server_group_id").hasArg().required()
                .desc("Agent server group id").build());
        options.addOption(Option.builder("s").longOpt("state").hasArg().required()
                .desc("Lifecycle state (Inactive|Active|PhasedOut|Removable)").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        CommandLine cli = context.getCLI();
        String id = cli.getOptionValue('i');
        InstanceGroupLifecycleState lifecycleState = StringExt.parseEnumIgnoreCase(cli.getOptionValue('s'), InstanceGroupLifecycleState.class);

        try {
            context.getAgentManagementClient()
                    .updateInstanceGroupLifecycleStatus(
                            id,
                            InstanceGroupLifecycleStatus.newBuilder()
                                    .withState(lifecycleState)
                                    .withDetail("Lifecycle update done by: " + System.getProperty("user.name"))
                                    .build()
                    ).block();
        } catch (Exception error) {
            logger.error("Failure", error);
        }
        logger.info("Updated");
    }
}