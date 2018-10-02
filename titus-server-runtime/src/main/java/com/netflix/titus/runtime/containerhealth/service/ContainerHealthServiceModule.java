package com.netflix.titus.runtime.containerhealth.service;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;

public final class ContainerHealthServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder.newSetBinder(binder(), ContainerHealthService.class).addBinding().to(AlwaysHealthyContainerHealthService.class);

        bind(ContainerHealthService.class).to(AggregatingContainerHealthService.class);
    }
}
