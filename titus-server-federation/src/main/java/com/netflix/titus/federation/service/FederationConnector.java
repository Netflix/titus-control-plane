package com.netflix.titus.federation.service;

import io.grpc.ManagedChannel;

import java.util.Optional;

public interface FederationConnector {
    Optional<ManagedChannel> getChannel();
}
