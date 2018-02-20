package io.netflix.titus.common.framework.fit.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitInjection;
import rx.Observable;

public class DefaultFitInjection implements FitInjection {

    private final String id;
    private final Class<? extends Throwable> exceptionType;

    private final ConcurrentMap<String, FitAction> actions = new ConcurrentHashMap<>();

    private DefaultFitInjection(InternalBuilder builder) {
        this.id = builder.id;
        this.exceptionType = builder.exceptionType;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void addAction(FitAction action) {
        actions.put(action.getId(), action);
    }

    @Override
    public Class<? extends Throwable> getExceptionType() {
        return exceptionType;
    }

    @Override
    public List<FitAction> getActions() {
        return new ArrayList<>(actions.values());
    }

    @Override
    public FitAction getAction(String actionId) {
        return Preconditions.checkNotNull(actions.get(actionId), "Action %s not found", actionId);
    }

    @Override
    public Optional<FitAction> findAction(String actionId) {
        return Optional.ofNullable(actions.get(actionId));
    }

    @Override
    public boolean removeAction(String actionId) {
        return actions.remove(actionId) != null;
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
        actions.values().forEach(a -> a.beforeImmediate(injectionPoint));
    }

    @Override
    public void afterImmediate(String injectionPoint) {
        actions.values().forEach(a -> a.afterImmediate(injectionPoint));
    }

    @Override
    public <T> Observable<T> beforeObservable(String injectionPoint, Supplier<Observable<T>> source) {
        Supplier<Observable<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.beforeObservable(injectionPoint, source);
        }
        return result.get();
    }

    @Override
    public <T> Observable<T> afterObservable(String injectionPoint, Supplier<Observable<T>> source) {
        Supplier<Observable<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.afterObservable(injectionPoint, source);
        }
        return result.get();
    }

    @Override
    public <T> CompletableFuture<T> beforeFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        Supplier<CompletableFuture<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.beforeCompletableFuture(injectionPoint, result);
        }
        return result.get();
    }

    @Override
    public <T> CompletableFuture<T> afterFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        Supplier<CompletableFuture<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.afterCompletableFuture(injectionPoint, result);
        }
        return result.get();
    }

    public static Builder newBuilder(String id) {
        return new InternalBuilder(id);
    }

    private static class InternalBuilder implements Builder {

        private final String id;
        private Class<? extends Throwable> exceptionType = RuntimeException.class;

        private InternalBuilder(String id) {
            this.id = id;
        }

        @Override
        public Builder withExceptionType(Class<? extends Throwable> exceptionType) {
            this.exceptionType = exceptionType;
            return this;
        }

        @Override
        public FitInjection build() {
            return new DefaultFitInjection(this);
        }
    }
}
