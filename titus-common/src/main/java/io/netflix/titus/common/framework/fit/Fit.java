package io.netflix.titus.common.framework.fit;

import io.netflix.titus.common.framework.fit.internal.DefaultFitComponent;
import io.netflix.titus.common.framework.fit.internal.DefaultFitInjection;
import io.netflix.titus.common.framework.fit.internal.DefaultFitRegistry;
import io.netflix.titus.common.framework.fit.internal.FitInvocationHandler;
import io.netflix.titus.common.framework.fit.internal.action.FitErrorAction;
import io.netflix.titus.common.util.tuple.Pair;

import static java.util.Arrays.asList;

public class Fit {

    private static final FitRegistry DEFAULT_REGISTRY = new DefaultFitRegistry(
            asList(
                    Pair.of(FitErrorAction.DESCRIPTOR, (id, properties) -> injection -> new FitErrorAction(id, properties, injection))
            )
    );

    public static FitRegistry getDefaultFitActionRegistry() {
        return DEFAULT_REGISTRY;
    }

    public static FitComponent newFitComponent(String id) {
        return new DefaultFitComponent(id);
    }

    public static FitInjection.Builder newFitInjectionBuilder(String id) {
        return DefaultFitInjection.newBuilder(id);
    }

    public static <I> I newFitProxy(I interf, FitInjection injection) {
        return FitInvocationHandler.newProxy(interf, injection);
    }
}
