package io.netflix.titus.common.framework.fit.internal.action;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netflix.titus.common.framework.fit.FitActionDescriptor;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.util.unit.TimeUnitExt;
import rx.Observable;

/**
 */
public class FitLatencyAction extends AbstractFitAction {

    public static final FitActionDescriptor DESCRIPTOR = new FitActionDescriptor(
            "latency",
            "Add a latency to request execution",
            ImmutableMap.of(
                    "before", "Insert latency before running the downstream action (defaults to 'true')",
                    "latency", "Latency duration (defaults to 100ms)",
                    "random", "If true, pick random value from range <0, latency>"
            )
    );

    private final Supplier<Long> latencyFun;

    public FitLatencyAction(String id, Map<String, String> properties, FitInjection injection) {
        super(id, DESCRIPTOR, properties, injection);
        long latencyMs = TimeUnitExt.parse(properties.getOrDefault("latency", "100ms"))
                .map(p -> p.getRight().toMillis(p.getLeft()))
                .orElseThrow(() -> new IllegalArgumentException("Invalid 'latency' parameter: " + properties.get("latency")));

        Preconditions.checkArgument(latencyMs > 0, "Latency must be > 0: %s", latencyMs);

        this.latencyFun = Boolean.parseBoolean(properties.getOrDefault("random", "false"))
                ? newRandomGen(latencyMs)
                : () -> latencyMs;
    }


    private Supplier<Long> newRandomGen(long latencyMs) {
        Random random = new Random();
        return () -> 1 + (random.nextLong() % latencyMs);
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
        if (runBefore) {
            doWait();
        }
    }

    @Override
    public void afterImmediate(String injectionPoint) {
        if (!runBefore) {
            doWait();
        }
    }

    @Override
    public <T> Supplier<Observable<T>> aroundObservable(String injectionPoint, Supplier<Observable<T>> source) {
        if (runBefore) {
            return () -> Observable.timer(latencyFun.get(), TimeUnit.MILLISECONDS).flatMap(tick -> source.get());
        }
        return () -> source.get().concatWith((Observable<T>) Observable.timer(latencyFun.get(), TimeUnit.MILLISECONDS).ignoreElements());
    }

    @Override
    public <T> Supplier<CompletableFuture<T>> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        if (runBefore) {
            return () -> {
                CompletableFuture<T> future = new CompletableFuture<>();
                Observable.timer(latencyFun.get(), TimeUnit.MILLISECONDS).subscribe(tick -> {
                    if (!future.isCancelled()) {
                        source.get().handle((result, error) -> {
                            if (error == null) {
                                future.complete(result);
                            } else {
                                future.completeExceptionally(error);
                            }
                            return result;
                        });
                    }
                });
                return future;
            };
        }
        return () -> {
            CompletableFuture<T> future = new CompletableFuture<>();
            source.get().handle((result, error) -> {
                if (error == null) {
                    Observable.timer(latencyFun.get(), TimeUnit.MILLISECONDS).subscribe(tick -> future.complete(result));
                } else {
                    future.completeExceptionally(error);
                }
                return result;
            });
            return future;
        };
    }

    @Override
    public <T> Supplier<ListenableFuture<T>> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source) {
        if (runBefore) {
            return () -> {
                SettableFuture<T> future = SettableFuture.create();
                Observable.timer(latencyFun.get(), TimeUnit.MILLISECONDS).subscribe(tick -> {
                    if (!future.isCancelled()) {
                        Futures.addCallback(source.get(), new FutureCallback<T>() {
                            @Override
                            public void onSuccess(@Nullable T result) {
                                future.set(result);
                            }

                            @Override
                            public void onFailure(Throwable error) {
                                future.setException(error);
                            }
                        });
                    }
                });
                return future;
            };
        }
        return () -> {
            SettableFuture<T> future = SettableFuture.create();
            Futures.addCallback(source.get(), new FutureCallback<T>() {
                @Override
                public void onSuccess(@Nullable T result) {
                    if (!future.isCancelled()) {
                        Observable.timer(latencyFun.get(), TimeUnit.MILLISECONDS).subscribe(tick -> future.set(result));
                    }
                }

                @Override
                public void onFailure(Throwable error) {
                    future.setException(error);
                }
            });
            return future;
        };
    }

    private void doWait() {
        try {
            Thread.sleep(latencyFun.get());
        } catch (InterruptedException ignore) {
        }
    }
}
