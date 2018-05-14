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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.cache.Cache;
import com.netflix.titus.common.util.cache.Caches;
import com.netflix.titus.common.util.rx.InstrumentedEventLoop;
import com.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultAgentResourceCache implements AgentResourceCache {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentResourceCache.class);

    private static final int MAX_CACHE_SIZE = 20_000;
    private static final String METRIC_NAME_ROOT = "titusMaster.scheduler.agentResourceCache";
    private static final String ACTION_NAME = "createOrUpdate";

    private final InstrumentedEventLoop eventLoop;
    private final Cache<String, AgentResourceCacheInstance> cache;

    @Inject
    public DefaultAgentResourceCache(Registry registry) {
        this(registry, Schedulers.computation());
    }

    public DefaultAgentResourceCache(Registry registry,
                                     Scheduler scheduler) {
        eventLoop = ObservableExt.createEventLoop(METRIC_NAME_ROOT, registry, scheduler);
        cache = Caches.instrumentedCacheWithMaxSize(MAX_CACHE_SIZE, METRIC_NAME_ROOT, registry);
    }

    @Override
    public void createOrUpdate(String hostname, Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> function) {
        eventLoop.schedule(ACTION_NAME, () -> {
            AgentResourceCacheInstance existingCacheInstance = cache.getIfPresent(hostname);
            AgentResourceCacheInstance newCacheInstance = function.apply(Optional.ofNullable(existingCacheInstance));
            if (!Objects.equals(newCacheInstance, existingCacheInstance)) {
                logger.debug("Creating or updating entry with hostname: {} and value: {}", hostname, newCacheInstance);
                cache.put(hostname, newCacheInstance);
            }
        });
    }

    @Override
    public Optional<AgentResourceCacheInstance> get(String hostname) {
        return Optional.ofNullable(cache.getIfPresent(hostname));
    }

    @Override
    public void shutdown() {
        eventLoop.shutdown();
        cache.shutdown();
    }
}
