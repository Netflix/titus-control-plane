package io.netflix.titus.common.framework.fit.internal;

import io.netflix.titus.common.framework.fit.FitFramework;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.framework.fit.FitRegistry;

public class ActiveFitFramework extends FitFramework {

    private static final String ROOT_COMPONENT = "root";

    private final FitRegistry fitRegistry;

    private final DefaultFitComponent rootComponent;

    public ActiveFitFramework(FitRegistry fitRegistry) {
        this.fitRegistry = fitRegistry;
        this.rootComponent = new DefaultFitComponent(ROOT_COMPONENT);
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public FitRegistry getFitRegistry() {
        return fitRegistry;
    }

    @Override
    public FitComponent getRootComponent() {
        return rootComponent;
    }

    @Override
    public FitInjection.Builder newFitInjectionBuilder(String id) {
        return DefaultFitInjection.newBuilder(id);
    }

    @Override
    public <I> I newFitProxy(I interf, FitInjection injection) {
        return FitInvocationHandler.newProxy(interf, injection);
    }
}
