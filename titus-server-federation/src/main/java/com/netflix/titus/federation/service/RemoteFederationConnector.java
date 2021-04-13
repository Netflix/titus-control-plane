package com.netflix.titus.federation.service;

import io.grpc.ManagedChannel;

public interface RemoteFederationConnector {
    ManagedChannel getChannel();
}
