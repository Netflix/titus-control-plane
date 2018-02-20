package io.netflix.titus.common.framework.fit.internal.action;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitActionDescriptor;
import rx.Observable;

/**
 * TODO finish implementation
 */
public class FitLatencyAction implements FitAction {

    private final long latencyMs;

    public FitLatencyAction(Map<String, Object> properties) {
        this.latencyMs = Long.parseLong((String) properties.get("latencyMs"));
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public FitActionDescriptor getDescriptor() {
        return null;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
    }

    @Override
    public void afterImmediate(String injectionPoint) {

    }

    @Override
    public <T> Supplier<Observable<T>> beforeObservable(String injectionPoint, Supplier<Observable<T>> source) {
        return null;
    }

    @Override
    public <T> Supplier<Observable<T>> afterObservable(String injectionPoint, Supplier<Observable<T>> source) {
        return null;
    }

    @Override
    public <T> Supplier<CompletableFuture<T>> beforeCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        return null;
    }

    @Override
    public <T> Supplier<CompletableFuture<T>> afterCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        return null;
    }
}
