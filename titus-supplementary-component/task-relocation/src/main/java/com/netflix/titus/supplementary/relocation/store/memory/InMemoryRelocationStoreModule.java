package com.netflix.titus.supplementary.relocation.store.memory;

import com.google.inject.AbstractModule;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationArchiveStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;

public class InMemoryRelocationStoreModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TaskRelocationStore.class).to(InMemoryTaskRelocationStore.class);
        bind(TaskRelocationArchiveStore.class).to(InMemoryTaskRelocationArchiveStore.class);
    }
}
