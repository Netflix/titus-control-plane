package com.netflix.titus.testkit.junit.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.rules.ExternalResource;

public class CloseableExternalResource extends ExternalResource {

    private final List<Runnable> autoCloseActions = new ArrayList<>();

    @Override
    protected void after() {
        autoCloseActions.forEach(a -> {
            try {
                a.run();
            } catch (Exception ignore) {
            }
        });
        super.after();
    }

    public <T> T autoCloseable(T value, Consumer<T> shutdownHook) {
        autoCloseActions.add(() -> shutdownHook.accept(value));
        return value;
    }
}
