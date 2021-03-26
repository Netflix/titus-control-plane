package com.netflix.titus.federation.service;

import com.netflix.titus.api.federation.model.Federation;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class DefaultFederationConnector implements FederationConnector {
    private final ManagedChannel channel;

    @Inject
    public DefaultFederationConnector(FederationInfoResolver fedInfoResolver) {
        Federation fed = fedInfoResolver.resolve();
        channel = NettyChannelBuilder.forTarget(fed.getAddress())
                .usePlaintext(true)
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .build();
    }

    @Override
    public Optional<ManagedChannel> getChannel() {
        return Optional.of(channel);
    }
}
