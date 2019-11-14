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

package com.netflix.titus.common.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.ThreadPoolMonitor;

/**
 * Factory methods and utilities for {@link java.util.concurrent.ExecutorService executors}.
 */
public final class ExecutorsExt {
    private ExecutorsExt() {
    }

    public static ExecutorService namedSingleThreadExecutor(String name) {
        return Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Unbounded elastic thread pool that grows and shrinks as necessary.
     */
    public static ExecutorService instrumentedCachedThreadPool(Registry registry, String name) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .setDaemon(true)
                .build();
        // similar to Executors.newCachedThreadPool(), but explicitly use the concrete ThreadPoolExecutor type to ensure
        // it can be instrumented by Spectator
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                threadFactory
        );
        ThreadPoolMonitor.attach(registry, executor, name);
        return executor;
    }

    /**
     * Fixed size thread pool that pre-allocates all threads. When no more threads are available, requests will block.
     */
    public static ExecutorService instrumentedFixedSizeThreadPool(Registry registry, String name, int size) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .setDaemon(true)
                .build();
        // similar to Executors.newFixedSizeThreadPool(), but explicitly use the concrete ThreadPoolExecutor type
        // to ensure it can be instrumented by Spectator
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                size, size,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory
        );
        ThreadPoolMonitor.attach(registry, executor, name);
        return executor;
    }

}
