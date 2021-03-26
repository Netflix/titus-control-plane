package com.netflix.titus.federation.service;

import com.netflix.titus.api.federation.model.Federation;

public interface FederationInfoResolver {
    Federation resolve();
}
