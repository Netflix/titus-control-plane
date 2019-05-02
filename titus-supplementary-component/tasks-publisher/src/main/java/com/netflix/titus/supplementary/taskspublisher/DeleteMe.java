/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.supplementary.taskspublisher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DeleteMe {
    private static final Logger logger = LoggerFactory.getLogger(DeleteMe.class);

    public static void main(String[] args) throws Exception {
//        tryGuavaCache();
        tryCaffeine();
    }

    private static void tryGuavaCache() throws Exception {
        LoadingCache<String, Integer> cache = CacheBuilder.newBuilder()
                .maximumSize(3)
                .build(new CacheLoader<String, Integer>() {
                    @Override
                    public Integer load(String key) throws Exception {
                        logger.info("Loading in thread {}", Thread.currentThread().getName());
                        Thread.sleep(5000);
                        return key.hashCode();
                    }
                });

        cache.put("f1", 1);
        cache.put("f2", 2);
        cache.put("f3", 3);
        cache.put("f4", 4);

        List<String> keys = Arrays.asList("f1", "f4", "f3");
        readValuesForKeys(cache, keys);

        Thread.sleep(5000);
        logger.info("\n");
        logger.info("Lets do this again -------- ");
        readValuesForKeys(cache, keys);

        Thread.sleep(5000);
        logger.info("\n");
        logger.info("Lets do this again -------- ");
        readValuesForKeys(cache, keys);

    }

    private static void tryCaffeine() {
        AtomicInteger value = new AtomicInteger(0);
        AsyncLoadingCache<String, Integer> asyncLoadingCache = Caffeine.newBuilder()
                .maximumSize(3)
                .buildAsync(new AsyncCacheLoader<String, Integer>() {
                    @Nonnull
                    @Override
                    public CompletableFuture<Integer> asyncLoad(@Nonnull String key, @Nonnull Executor executor) {
                        CompletableFuture<Integer> returnValue = new CompletableFuture<>();
                        new Thread(() -> {
                            try {
                                Thread.sleep(1000);
                                logger.info("Async Loading for {} in thread {}", key, Thread.currentThread().getName());
                                returnValue.complete(value.incrementAndGet());
                            } catch (InterruptedException e) {
                                logger.error("Exception in sleep", e);
                            }

                        }).run();
                        return returnValue;
                    }
                });
        asyncLoadingCache.put("k1", CompletableFuture.completedFuture(100));
        asyncLoadingCache.put("k2", CompletableFuture.completedFuture(200));
        asyncLoadingCache.put("k3", CompletableFuture.completedFuture(300));
        asyncLoadingCache.put("k4", CompletableFuture.completedFuture(400));
        readValuesForKeyAsync(asyncLoadingCache, Arrays.asList("k1", "k4", "k2", "k3"));
    }

    private static void readValuesForKeyAsync(AsyncLoadingCache<String, Integer> cache, List<String> keys) {
        Flux.fromIterable(keys)
                .flatMap(k -> Mono.fromFuture(cache.get(k))
                        .doOnNext(v -> {
                            logger.info("v({}) = {} in thread {}", k, v, Thread.currentThread().getName());
                        }))
                .subscribe();
    }

    private static void readValuesForKeys2(com.github.benmanes.caffeine.cache.LoadingCache<String, Integer> cache, List<String> keys) {
        keys.forEach(key -> {
            logger.info("Asking in thread {}", Thread.currentThread().getName());
            Integer value = cache.get(key);
            logger.info("v({}) = {} in thread {}", key, value, Thread.currentThread().getName());
        });
    }


    private static void readValuesForKeys(LoadingCache<String, Integer> cache, List<String> keys) {
        keys.forEach(key -> {
            logger.info("Asking in thread {}", Thread.currentThread().getName());

            try {
                Integer value = cache.get(key);
                logger.info("v({}) = {} in thread {}", key, value, Thread.currentThread().getName());
            } catch (ExecutionException e) {
                logger.error("Exception getting value from cache {}", key, e);
            }
        });
    }


}
