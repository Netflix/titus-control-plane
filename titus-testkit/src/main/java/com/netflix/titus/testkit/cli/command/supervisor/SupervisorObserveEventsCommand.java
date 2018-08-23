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

package com.netflix.titus.testkit.cli.command.supervisor;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceBlockingStub;
import com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.grpc.GrpcClientErrorUtils;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class SupervisorObserveEventsCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(SupervisorObserveEventsCommand.class);

    @Override
    public String getDescription() {
        return "observe supervisor events";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        SupervisorServiceBlockingStub stub = GrpcClientErrorUtils.attachCallHeaders(SupervisorServiceGrpc.newBlockingStub(context.createChannel()));
        stub.observeEvents(Empty.getDefaultInstance())
                .forEachRemaining(event -> {
                            switch (event.getEventCase()) {
                                case MASTERINSTANCEUPDATE:
                                    logger.info("Add/updated: {}", SupervisorGrpcModelConverters.toCoreMasterInstance(event.getMasterInstanceUpdate().getInstance()));
                                    break;
                                case MASTERINSTANCEREMOVED:
                                    logger.info("Removed: {}", event.getMasterInstanceRemoved().getInstanceId());
                                    break;
                            }
                        }
                );
    }
}
