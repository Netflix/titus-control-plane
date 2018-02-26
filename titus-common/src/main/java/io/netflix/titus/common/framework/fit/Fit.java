package io.netflix.titus.common.framework.fit;

import java.util.function.Supplier;

import io.netflix.titus.common.framework.fit.internal.DefaultFitComponent;
import io.netflix.titus.common.framework.fit.internal.DefaultFitInjection;
import io.netflix.titus.common.framework.fit.internal.DefaultFitRegistry;
import io.netflix.titus.common.framework.fit.internal.FitInvocationHandler;
import io.netflix.titus.common.framework.fit.internal.action.FitErrorAction;
import io.netflix.titus.common.framework.fit.internal.action.FitLatencyAction;
import io.netflix.titus.common.util.tuple.Pair;

import static java.util.Arrays.asList;

/**
 * Factory methods for the FIT framework.
 */
public class Fit {

    private static final FitRegistry DEFAULT_REGISTRY = new DefaultFitRegistry(
            asList(
                    Pair.of(FitErrorAction.DESCRIPTOR, (id, properties) -> injection -> new FitErrorAction(id, properties, injection)),
                    Pair.of(FitLatencyAction.DESCRIPTOR, (id, properties) -> injection -> new FitLatencyAction(id, properties, injection))
            )
    );

    /**
     * Returns the default FIT action registry, with the predefined/standard action set.
     */
    public static FitRegistry getDefaultFitActionRegistry() {
        return DEFAULT_REGISTRY;
    }

    /**
     * Creates a new, empty FIT component.
     */
    public static FitComponent newFitComponent(String id) {
        return new DefaultFitComponent(id);
    }

    /**
     * Creates a {@link FitInjection} builder.
     */
    public static FitInjection.Builder newFitInjectionBuilder(String id) {
        return DefaultFitInjection.newBuilder(id);
    }

    /**
     * Creates an interface proxy with methods instrumented using the provided {@link FitInjection} instance:
     * <ul>
     * <li>
     * All methods returning {@link java.util.concurrent.CompletableFuture} are wrapped with
     * {@link FitInjection#aroundCompletableFuture(String, Supplier)} operator.
     * </li>
     * <li>
     * All methods returning {@link com.google.common.util.concurrent.ListenableFuture} are wrapped with
     * {@link FitInjection#aroundListenableFuture(String, Supplier)} operator.
     * </li>
     * <li>
     * All methods returning {@link rx.Observable} are wrapped with
     * {@link FitInjection#aroundObservable(String, Supplier)} operator.
     * </li>
     * <li>
     * All the remaining interface methods are assumed to be blocking, and are wrapped with
     * {@link FitInjection#beforeImmediate(String)}, and {@link FitInjection#afterImmediate(String)} handlers.
     * </li>
     * </ul>
     */
    public static <I> I newFitProxy(I interf, FitInjection injection) {
        return FitInvocationHandler.newProxy(interf, injection);
    }
}
