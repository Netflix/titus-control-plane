package com.netflix.titus.common.util.rx;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

class ReactorReEmitterOperator<T> implements Function<Flux<T>, Publisher<T>> {

    private final Function<T, T> transformer;
    private final long intervalMs;
    private final Scheduler scheduler;

    ReactorReEmitterOperator(Function<T, T> transformer, long interval, TimeUnit timeUnit, Scheduler scheduler) {
        this.transformer = transformer;
        this.intervalMs = timeUnit.toMillis(interval);
        this.scheduler = scheduler;
    }

    @Override
    public Publisher<T> apply(Flux<T> source) {
        return Flux.create(emitter -> {
            AtomicReference<T> lastItemRef = new AtomicReference<>();
            AtomicReference<Long> reemmitDeadlineRef = new AtomicReference<>();

            Flux<T> reemiter = Flux.interval(Duration.ofMillis(intervalMs), Duration.ofMillis(intervalMs), scheduler)
                    .flatMap(tick -> {
                        if (lastItemRef.get() != null && reemmitDeadlineRef.get() <= scheduler.now(TimeUnit.MILLISECONDS)) {
                            return Flux.just(transformer.apply(lastItemRef.get()));
                        }
                        return Flux.empty();
                    });

            Flux.merge(
                    source.doOnNext(next -> {
                        lastItemRef.set(next);
                        reemmitDeadlineRef.set(scheduler.now(TimeUnit.MILLISECONDS) + intervalMs);
                    }),
                    reemiter
            ).subscribe(emitter::next, emitter::error, emitter::complete);
        });
    }
}
