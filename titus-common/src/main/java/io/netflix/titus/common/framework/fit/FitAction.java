package io.netflix.titus.common.framework.fit;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;

/**
 * Syntactic failure generator, like exception or latency. Each action is configured to trigger either before or
 * after client code section. For example an action with before=true property, may trigger an error when {@link #beforeImmediate(String)}
 * method is called, but at the same time calls to {@link #afterImmediate(String)} are always void.
 */
public interface FitAction {

    /**
     * Unique action identifier with an injection point.
     */
    String getId();

    /**
     * Action kind descriptor.
     */
    FitActionDescriptor getDescriptor();

    /**
     * Action configuration data.
     */
    Map<String, String> getProperties();

    /**
     * Method to be called in the beginning of the client's code section.
     */
    void beforeImmediate(String injectionPoint);

    /**
     * Method to be called at the end of the client's code section.
     */
    void afterImmediate(String injectionPoint);

    /**
     * Method to be called at the end of the client's code section.
     *
     * @param result value returned by the client, which can be intercepted and modified by the FIT action
     */
    default <T> T afterImmediate(String injectionPoint, T result) {
        afterImmediate(injectionPoint);
        return result;
    }

    /**
     * Wraps an observable to inject on subscribe or after completion errors.
     */
    <T> Supplier<Observable<T>> aroundObservable(String injectionPoint, Supplier<Observable<T>> source);

    /**
     * Wraps a Java future to inject an error before the future creation or after it completes.
     */
    <T> Supplier<CompletableFuture<T>> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source);

    /**
     * Wraps a Guava future to inject an error before the future creation or after it completes.
     */
    <T> Supplier<ListenableFuture<T>> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source);
}
