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

package com.netflix.titus.cli.command.eviction;

import java.util.Iterator;

import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.cli.CliCommand;
import com.netflix.titus.cli.CommandContext;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvictionEventsCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(EvictionEventsCommand.class);

    @Override
    public String getDescription() {
        return "observe eviction event stream";
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
        Iterator<EvictionServiceEvent> it = EvictionServiceGrpc.newBlockingStub(context.createChannel()).observeEvents(
                ObserverEventRequest.newBuilder()
                        .setIncludeSnapshot(true)
                        .build()
        );

        while (it.hasNext()) {
            logger.info("Emitted: {}", it.next());
        }
    }
}
