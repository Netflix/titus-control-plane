package com.netflix.titus.common.network.reverseproxy.grpc;

import java.util.Optional;

import javax.inject.Singleton;

import io.grpc.ManagedChannel;

@Singleton
public class NoOpManagedChannelFactory implements ManagedChannelFactory {

    @Override
    public Optional<ManagedChannel> newManagedChannel(String serviceName) {
        return Optional.empty();
    }
}
