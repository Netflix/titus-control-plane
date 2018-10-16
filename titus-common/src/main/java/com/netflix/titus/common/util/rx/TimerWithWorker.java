/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
