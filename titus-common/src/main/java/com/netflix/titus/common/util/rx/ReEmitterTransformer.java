package com.netflix.titus.common.util.rx;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import rx.Observable;
import rx.Scheduler;

public class ReEmitterTransformer<T> implements Observable.Transformer<T, T> {

    private final Function<T, T> transformer;
    private final long intervalMs;
    private final Scheduler scheduler;

    ReEmitterTransformer(Function<T, T> transformer, long interval, TimeUnit timeUnit, Scheduler scheduler) {
        this.transformer = transformer;
        this.intervalMs = timeUnit.toMillis(interval);
        this.scheduler = scheduler;
    }

    @Override
    public Observable<T> call(Observable<T> source) {
        return Observable.unsafeCreate(subscriber -> {
            AtomicReference<T> lastItemRef = new AtomicReference<>();
            AtomicReference<Long> reemmitDeadlineRef = new AtomicReference<>();

            Observable<T> reemiter = Observable.interval(intervalMs, intervalMs, TimeUnit.MILLISECONDS, scheduler).flatMap(tick -> {
                if (lastItemRef.get() != null && reemmitDeadlineRef.get() <= scheduler.now()) {
                    return Observable.just(transformer.apply(lastItemRef.get()));
                }
                return Observable.empty();
            });

            Observable.merge(
                    source.doOnNext(next -> {
                        lastItemRef.set(next);
                        reemmitDeadlineRef.set(scheduler.now() + intervalMs);
                    }),
                    reemiter
            ).subscribe(subscriber);
        });
    }
}
