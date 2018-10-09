package com.netflix.titus.supplementary.relocation.workflow;

import com.google.inject.AbstractModule;

public final class RelocationWorkflowModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RelocationWorkflowExecutor.class).asEagerSingleton();
    }
}
