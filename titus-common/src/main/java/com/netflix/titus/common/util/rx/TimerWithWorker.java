package com.netflix.titus.common.util.rx;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

class TimerWithWorker {

    static Mono<Void> timer(Runnable action, Scheduler.Worker worker, Duration delay) {
        return timer(() -> {
            action.run();
            return null;
        }, worker, delay);
    }

    static <T> Mono<T> timer(Supplier<T> action, Scheduler.Worker worker, Duration delay) {
        if (worker.isDisposed()) {
            return Mono.empty();
        }

        return Mono.create(sink -> {
            if (worker.isDisposed()) {
                sink.success();
            } else {
                timer(action, worker, delay, sink);
            }
        });
    }

    private static <T> void timer(Supplier<T> action, Scheduler.Worker worker, Duration delay, MonoSink<T> sink) {
        worker.schedule(() -> {
            try {
                T result = action.get();
                if (result != null) {
                    sink.success(result);
                } else {
                    sink.success();
                }
            } catch (Exception e) {
                sink.error(e);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }
}
