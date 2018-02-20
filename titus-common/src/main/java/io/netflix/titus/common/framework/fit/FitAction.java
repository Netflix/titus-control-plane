package io.netflix.titus.common.framework.fit;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import rx.Observable;

public interface FitAction {

    String getId();

    FitActionDescriptor getDescriptor();

    Map<String, String> getProperties();

    void beforeImmediate(String injectionPoint);

    void afterImmediate(String injectionPoint);

    <T> Supplier<Observable<T>> beforeObservable(String injectionPoint, Supplier<Observable<T>> source);

    <T> Supplier<Observable<T>> afterObservable(String injectionPoint, Supplier<Observable<T>> source);

    <T> Supplier<CompletableFuture<T>> beforeCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source);

    <T> Supplier<CompletableFuture<T>> afterCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source);
}
