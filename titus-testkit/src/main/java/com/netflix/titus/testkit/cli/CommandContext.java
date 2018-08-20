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

package com.netflix.titus.testkit.cli;

import java.util.ArrayList;
import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

/**
 */
public class CommandContext {

    private static final int DEFAULT_PORT = 8980;

    protected final CommandLine commandLine;
    protected final String region;
    protected final String host;
    protected final int port;
    protected final List<ManagedChannel> channels = new ArrayList<>();

    public CommandContext(CommandLine commandLine) {
        this.commandLine = commandLine;
        this.region = commandLine.hasOption('r') ? commandLine.getOptionValue('r') : "us-east-1";
        this.host = commandLine.getOptionValue('H');
        this.port = resolvePort();
    }

    public CommandLine getCLI() {
        return commandLine;
    }

    public String getRegion() {
        return region;
    }

    public synchronized ManagedChannel createChannel() {
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true);
        if (channelBuilder instanceof NettyChannelBuilder) {
            NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) channelBuilder;
            nettyChannelBuilder.maxHeaderListSize(128 * 1024);
        }
        ManagedChannel channel = channelBuilder.build();

        channels.add(channel);
        return channel;
    }

    public void shutdown() {
        channels.forEach(ManagedChannel::shutdown);
    }

    private int resolvePort() {
        int port;
        try {
            port = commandLine.hasOption('p') ? ((Number) commandLine.getParsedOptionValue("p")).intValue() : DEFAULT_PORT;
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid port number " + commandLine.getOptionValue("p"));
        }
        return port;
    }
}
