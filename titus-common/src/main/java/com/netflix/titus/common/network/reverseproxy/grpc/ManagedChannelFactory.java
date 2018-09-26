package com.netflix.titus.common.network.reverseproxy.grpc;

import java.util.Optional;

import io.grpc.ManagedChannel;

public interface ManagedChannelFactory {

    /**
     * Return a {@link ManagedChannel} instance for a service with the given name.
     */
    Optional<ManagedChannel> newManagedChannel(String serviceName);
}
