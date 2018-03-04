package io.netflix.titus.common.framework.fit.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitInjection;
import rx.Observable;

public class DefaultFitInjection implements FitInjection {

    private final String id;
    private final String description;
    private final Class<? extends Throwable> exceptionType;

    private final ConcurrentMap<String, FitAction> actions = new ConcurrentHashMap<>();

    private DefaultFitInjection(InternalBuilder builder) {
        this.id = builder.id;
        this.description = builder.description;
        this.exceptionType = builder.exceptionType;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getDescription() {
        return description;
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
        FitAction action = actions.get(actionId);
        Preconditions.checkArgument(action != null, "Action %s not found", actionId);
        return action;
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
    public <T> T afterImmediate(String injectionPoint, T result) {
        for (FitAction fitAction : actions.values()) {
            result = fitAction.afterImmediate(injectionPoint, result);
        }
        return result;
    }

    @Override
    public <T> Observable<T> aroundObservable(String injectionPoint, Supplier<Observable<T>> source) {
        Supplier<Observable<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.aroundObservable(injectionPoint, source);
        }
        return result.get();
    }

    @Override
    public <T> CompletableFuture<T> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        Supplier<CompletableFuture<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.aroundCompletableFuture(injectionPoint, result);
        }
        return result.get();
    }

    @Override
    public <T> ListenableFuture<T> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source) {
        Supplier<ListenableFuture<T>> result = source;
        for (FitAction action : actions.values()) {
            result = action.aroundListenableFuture(injectionPoint, result);
        }
        return result.get();
    }

    public static Builder newBuilder(String id) {
        return new InternalBuilder(id);
    }

    private static class InternalBuilder implements Builder {

        private final String id;
        private Class<? extends Throwable> exceptionType = RuntimeException.class;
        private String description = "Not provided";

        private InternalBuilder(String id) {
            this.id = id;
        }


        @Override
        public Builder withDescription(String description) {
            this.description = description;
            return this;
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
