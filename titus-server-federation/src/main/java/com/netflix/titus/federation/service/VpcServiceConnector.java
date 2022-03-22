package com.netflix.titus.federation.service;

import io.grpc.ManagedChannel;

public interface VpcServiceConnector {
     ManagedChannel getChannel();
}
