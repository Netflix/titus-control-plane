package com.netflix.titus.federation.service;

import com.netflix.titus.api.federation.model.RemoteFederation;

public interface RemoteFederationInfoResolver {
    RemoteFederation resolve();
}
