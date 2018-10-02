package com.netflix.titus.ext.eureka.containerhealth;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;

public final class EurekaContainerHealthModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder.newSetBinder(binder(), ContainerHealthService.class).addBinding().to(EurekaContainerHealthService.class);
    }
}
