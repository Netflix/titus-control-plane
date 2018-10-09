package com.netflix.titus.supplementary.relocation.descheduler;

import com.google.inject.AbstractModule;

public class DeschedulerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DeschedulerService.class).to(DefaultDeschedulerService.class);
    }
}
