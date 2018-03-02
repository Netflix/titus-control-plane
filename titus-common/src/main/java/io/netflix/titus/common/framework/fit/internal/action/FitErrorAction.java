package io.netflix.titus.common.framework.fit.internal.action;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netflix.titus.common.framework.fit.AbstractFitAction;
import io.netflix.titus.common.framework.fit.FitActionDescriptor;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.util.ErrorGenerator;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.unit.TimeUnitExt;
import rx.Observable;

public class FitErrorAction extends AbstractFitAction {

    public static final FitActionDescriptor DESCRIPTOR = new FitActionDescriptor(
            "exception",
            "Throw an exception",
            ImmutableMap.of(
                    "before", "Throw exception before running the downstream action (defaults to 'true')",
                    "percentage", "Percentage of requests to fail (defaults to 50)",
                    "errorTime", "Time window during which requests will fail (defaults to '1m')",
                    "upTime", "Time window during which no errors occur (defaults to '1m')"
            )
    );

    private final double expectedRatio;

    private final Constructor<? extends Throwable> exceptionConstructor;
    private final ErrorGenerator errorGenerator;

    public FitErrorAction(String id, Map<String, String> properties, FitInjection injection) {
        super(id, DESCRIPTOR, properties, injection);

        this.expectedRatio = Double.parseDouble(properties.getOrDefault("percentage", "100")) / 100;
        long errorTimeMs = TimeUnitExt.parse(properties.getOrDefault("errorTime", "1m"))
                .map(p -> p.getRight().toMillis(p.getLeft()))
                .orElseThrow(() -> new IllegalArgumentException("Invalid 'errorTime' parameter: " + properties.get("errorTime")));
        long upTimeMs = TimeUnitExt.parse(properties.getOrDefault("upTime", "1m"))
                .map(p -> p.getRight().toMillis(p.getLeft()))
                .orElseThrow(() -> new IllegalArgumentException("Invalid 'upTime' parameter: " + properties.get("upTime")));

        Preconditions.checkArgument(upTimeMs > 0 || errorTimeMs > 0, "Both upTime and errorTime cannot be equal to 0");

        this.errorGenerator = ErrorGenerator.periodicFailures(upTimeMs, errorTimeMs, expectedRatio, Clocks.system());

        try {
            this.exceptionConstructor = getInjection().getExceptionType().getConstructor(String.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Invalid exception type: " + getInjection().getExceptionType());
        }
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
        if (runBefore) {
            doFail(injectionPoint);
        }
    }

    @Override
    public void afterImmediate(String injectionPoint) {
        if (!runBefore) {
            doFail(injectionPoint);
        }
    }

    @Override
    public <T> Supplier<Observable<T>> aroundObservable(String injectionPoint, Supplier<Observable<T>> source) {
        if (runBefore) {
            return () -> Observable.defer(() -> {
                doFail(injectionPoint);
                return source.get();
            });
        }
        return () -> source.get().concatWith(Observable.defer(() -> {
            doFail(injectionPoint);
            return Observable.empty();
        }));
    }

    @Override
    public <T> Supplier<CompletableFuture<T>> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        if (runBefore) {
            return () -> {
                if (errorGenerator.shouldFail()) {
                    CompletableFuture<T> future = new CompletableFuture<>();
                    future.completeExceptionally(newException(injectionPoint));
                    return future;
                }
                return source.get();
            };
        }
        return () -> source.get().thenApply(result -> doFail(injectionPoint) ? null : result);
    }

    @Override
    public <T> Supplier<ListenableFuture<T>> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source) {
        if (runBefore) {
            return () -> {
                if (errorGenerator.shouldFail()) {
                    return Futures.immediateFailedFuture(newException(injectionPoint));
                }
                return source.get();
            };
        }
        return () -> Futures.transform(source.get(), (Function<T, T>) input -> doFail(injectionPoint) ? null : input);
    }

    private boolean doFail(String injectionPoint) {
        if (errorGenerator.shouldFail()) {
            Throwable exception = newException(injectionPoint);
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            }
            throw new RuntimeException("Wrapper", exception);
        }
        return false;
    }

    private Throwable newException(String injectionPoint) {
        try {
            return exceptionConstructor.newInstance("FIT exception at " + injectionPoint);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create FIT exception from type " + getInjection().getExceptionType(), e);
        }
    }
}
