/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import com.netflix.titus.common.util.grpc.reactor.client.ReactorToGrpcClientBuilder;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.JobConnectorConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.ReactorJobManagementServiceStub;
import com.netflix.titus.runtime.connector.jobmanager.RemoteJobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.RemoteJobManagementClientWithKeepAlive;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CommonCallMetadataUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServiceDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

/**
 *
 */
public class CommandContext {

    private static final int DEFAULT_PORT = 8980;

    protected final TitusRuntime titusRuntime = TitusRuntimes.internal();

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

    public TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }

    public String getRegion() {
        return region;
    }

    public synchronized ManagedChannel createChannel() {
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext();
        if (channelBuilder instanceof NettyChannelBuilder) {
            NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) channelBuilder;
            nettyChannelBuilder.maxInboundMetadataSize(128 * 1024);
        }
        ManagedChannel channel = channelBuilder.build();

        channels.add(channel);
        return channel;
    }

    public CallMetadata getCallMetadata(String reason) {
        return CallMetadata.newBuilder()
                .withCallers(Collections.singletonList(Caller.newBuilder().withId("cli").build()))
                .withCallReason(reason)
                .build();
    }

    public GrpcToReactorClientFactory getGrpcToReactorClientFactory() {
        return new GrpcToReactorClientFactory() {
            @Override
            public <GRPC_STUB extends AbstractStub<GRPC_STUB>, REACT_API> REACT_API apply(GRPC_STUB stub, Class<REACT_API> apiType, ServiceDescriptor serviceDescriptor) {
                return ReactorToGrpcClientBuilder
                        .<REACT_API, GRPC_STUB, CallMetadata>newBuilder(
                                apiType, stub, serviceDescriptor, CallMetadata.class
                        )
                        .withGrpcStubDecorator(CommonCallMetadataUtils.newGrpcStubDecorator(AnonymousCallMetadataResolver.getInstance()))
                        .withTimeout(Duration.ofMillis(GrpcRequestConfiguration.DEFAULT_REQUEST_TIMEOUT_MS))
                        .withStreamingTimeout(Duration.ofMillis(GrpcRequestConfiguration.DEFAULT_STREAMING_TIMEOUT_MS))
                        .build();
            }
        };
    }

    public JobManagementServiceGrpc.JobManagementServiceStub getJobManagementGrpcStub() {
        return JobManagementServiceGrpc.newStub(createChannel());
    }

    public JobManagementServiceGrpc.JobManagementServiceBlockingStub getJobManagementGrpcBlockingStub() {
        return JobManagementServiceGrpc.newBlockingStub(createChannel());
    }

    public RemoteJobManagementClient getJobManagementClient() {
        ReactorJobManagementServiceStub reactorStub = getGrpcToReactorClientFactory().apply(
                getJobManagementGrpcStub(),
                ReactorJobManagementServiceStub.class,
                JobManagementServiceGrpc.getServiceDescriptor()
        );
        return new RemoteJobManagementClient("cli", reactorStub, titusRuntime);
    }

    public RemoteJobManagementClient getJobManagementClientWithKeepAlive(long keepAliveInternalMs) {
        JobManagementServiceGrpc.JobManagementServiceStub stub = getJobManagementGrpcStub();
        ReactorJobManagementServiceStub reactorStub = getGrpcToReactorClientFactory().apply(
                stub,
                ReactorJobManagementServiceStub.class,
                JobManagementServiceGrpc.getServiceDescriptor()
        );
        JobConnectorConfiguration configuration = Archaius2Ext.newConfiguration(JobConnectorConfiguration.class,
                "titus.connector.jobService.keepAliveIntervalMs", "" + keepAliveInternalMs
        );
        return new RemoteJobManagementClientWithKeepAlive("cli", configuration, stub, reactorStub, titusRuntime);
    }

    public SchedulerServiceGrpc.SchedulerServiceBlockingStub getSchedulerServiceBlockingStub() {
        return SchedulerServiceGrpc.newBlockingStub(createChannel());
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
