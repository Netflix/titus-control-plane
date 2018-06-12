package com.netflix.titus.master.eviction.service;

import com.google.inject.AbstractModule;
import com.netflix.titus.api.eviction.service.EvictionOperations;

public class EvictionServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EvictionOperations.class).to(DefaultEvictionOperations.class);
    }
}
