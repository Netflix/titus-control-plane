package io.netflix.titus.common.framework.fit.internal.action;

import java.util.Map;

import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitActionDescriptor;
import io.netflix.titus.common.framework.fit.FitInjection;

public abstract class AbstractFitAction implements FitAction {

    private final String id;
    private final FitActionDescriptor descriptor;
    private final Map<String, String> properties;
    private final FitInjection injection;

    protected AbstractFitAction(String id, FitActionDescriptor descriptor, Map<String, String> properties, FitInjection injection) {
        this.id = id;
        this.descriptor = descriptor;
        this.properties = properties;
        this.injection = injection;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public FitActionDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public FitInjection getInjection() {
        return injection;
    }
}
