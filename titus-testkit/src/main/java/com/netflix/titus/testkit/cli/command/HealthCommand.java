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

package com.netflix.titus.testkit.cli.command;

import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCommand implements CliCommand {
    private static final Logger logger = LoggerFactory.getLogger(HealthCommand.class);

    @Override
    public String getDescription() {
        return "Get health status";
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
    public void execute(CommandContext context) {
        HealthGrpc.HealthBlockingStub client = HealthGrpc.newBlockingStub(context.createChannel());
        try {
            HealthCheckResponse response = client.check(HealthCheckRequest.newBuilder().build());
            logger.info(PrettyPrinters.print(response));
        } catch (Exception e) {
            ErrorReports.handleReplyError("Error querying status", e);
        }
    }
}
