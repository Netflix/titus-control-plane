package com.netflix.titus.federation.service;

import com.netflix.titus.api.federation.model.RemoteFederation;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;

import javax.inject.Inject;

public class DefaultRemoteFederationInfoResolver implements RemoteFederationInfoResolver {

    private final TitusFederationConfiguration configuration;

    @Inject
    public DefaultRemoteFederationInfoResolver(TitusFederationConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public RemoteFederation resolve() {
        return new RemoteFederation(configuration.getRemoteFederation());
    }
}
