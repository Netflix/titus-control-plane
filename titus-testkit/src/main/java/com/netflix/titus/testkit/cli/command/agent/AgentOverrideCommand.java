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

import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.InstanceOverrideState;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.rx.RxGrpcAgentManagementService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentOverrideCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(AgentObserveCommand.class);

    @Override
    public String getDescription() {
        return "Set agent override status";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("instance_id").hasArg().required()
                .desc("Agent instance id").build());
        options.addOption(Option.builder("s").longOpt("state").hasArg().required()
                .desc("Override state (None|Quarantined)").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();
        String instanceId = cli.getOptionValue('i');
        InstanceOverrideState overrideState = StringExt.parseEnumIgnoreCase(cli.getOptionValue('s'), InstanceOverrideState.class);

        Throwable error = new RxGrpcAgentManagementService(context.createChannel())
                .updateInstanceOverride(InstanceOverrideStateUpdate.newBuilder()
                        .setOverrideState(overrideState)
                        .setAgentInstanceId(instanceId)
                        .setDetail("Override done by " + System.getProperty("user.name"))
                        .build()
                ).get();
        if (error == null) {
            logger.info("Updated");
        } else {
            logger.error("Failure", error);
        }
    }
}