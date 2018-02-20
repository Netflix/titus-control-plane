package io.netflix.titus.common.framework.fit;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import rx.Observable;

public interface FitInjection {

    String getId();

    Class<? extends Throwable> getExceptionType();

    void addAction(FitAction action);

    List<FitAction> getActions();

    FitAction getAction(String actionId);

    Optional<FitAction> findAction(String actionId);

    boolean removeAction(String actionId);

    void beforeImmediate(String injectionPoint);

    void afterImmediate(String injectionPoint);

    <T> Observable<T> beforeObservable(String injectionPoint, Supplier<Observable<T>> source);

    <T> Observable<T> afterObservable(String injectionPoint, Supplier<Observable<T>> source);

    <T> CompletableFuture<T> beforeFuture(String injectionPoint, Supplier<CompletableFuture<T>> source);

    <T> CompletableFuture<T> afterFuture(String injectionPoint, Supplier<CompletableFuture<T>> source);

    interface Builder {

        Builder withExceptionType(Class<? extends Throwable> exceptionType);

        FitInjection build();
    }
}
