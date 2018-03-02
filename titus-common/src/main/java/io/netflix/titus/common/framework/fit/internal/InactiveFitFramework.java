package io.netflix.titus.common.framework.fit.internal;

import io.netflix.titus.common.framework.fit.FitFramework;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.framework.fit.FitRegistry;

public class InactiveFitFramework extends FitFramework {
    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public FitRegistry getFitRegistry() {
        throw new IllegalStateException("FIT framework not activated");
    }

    @Override
    public FitComponent getRootComponent() {
        throw new IllegalStateException("FIT framework not activated");
    }

    @Override
    public FitInjection.Builder newFitInjectionBuilder(String id) {
        throw new IllegalStateException("FIT framework not activated");
    }

    @Override
    public <I> I newFitProxy(I interf, FitInjection injection) {
        throw new IllegalStateException("FIT framework not activated");
    }
}
