package com.netflix.titus.common.util.rx;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import rx.Observable;

/**
 * Supplementary Spring Reactor operators.
 */
public final class ReactorExt {

    private ReactorExt() {
    }

    /**
     * If the source observable does not emit any item in the configured amount of time, the last emitted value is
     * re-emitted again, optionally updated by the transformer.
     */
    public static <T> Function<Flux<T>, Publisher<T>> reEmitter(Function<T, T> transformer, long interval, TimeUnit timeUnit, Scheduler scheduler) {
        return new ReactorReEmitterOperator<>(transformer, interval, timeUnit, scheduler);
    }

    /**
     * RxJava {@link Observable} to {@link Flux} bridge.
     */
    public static <T> Flux<T> toFlux(Observable<T> observable) {
        return Flux.create(new FluxObservableEmitter<>(observable));
    }
}
