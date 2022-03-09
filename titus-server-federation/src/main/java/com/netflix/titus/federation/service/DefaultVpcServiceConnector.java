package com.netflix.titus.federation.service;

import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class DefaultVpcServiceConnector implements VpcServiceConnector {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRemoteFederationConnector.class);
    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;
    private final TitusFederationConfiguration configuration;
    private final ManagedChannel channel;

    @Inject
    public DefaultVpcServiceConnector(TitusFederationConfiguration configuration) {
        this.configuration = configuration;
        String []hostPort = configuration.getVpcService().split(":");

        channel = NettyChannelBuilder
                .forAddress(hostPort[0], Integer.parseInt(hostPort[1]))
                .usePlaintext()
                .defaultLoadBalancingPolicy("round_robin")
                .build();
    }

    @Override
    public ManagedChannel getChannel() {
        return null;
    }
}
