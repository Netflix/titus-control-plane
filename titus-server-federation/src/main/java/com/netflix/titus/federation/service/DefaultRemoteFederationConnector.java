package com.netflix.titus.federation.service;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public class DefaultRemoteFederationConnector implements RemoteFederationConnector {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRemoteFederationConnector.class);

    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;
    private final ManagedChannel channel;

    @Inject
    public DefaultRemoteFederationConnector(RemoteFederationInfoResolver remoteFederationInfoResolver) {
        channel = NettyChannelBuilder.forTarget(remoteFederationInfoResolver.resolve().getAddress())
                .usePlaintext(true)
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .build();
    }

    @Override
    public ManagedChannel getChannel() {
        return channel;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("shutting down gRPC channels");
        try {
            channel.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            if (!channel.isTerminated()) {
                channel.shutdownNow();
            }
        }
    }
}
