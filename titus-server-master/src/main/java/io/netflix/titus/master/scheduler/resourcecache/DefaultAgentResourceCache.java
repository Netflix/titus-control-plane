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

package io.netflix.titus.master.scheduler.resourcecache;

import java.util.Optional;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.cache.Cache;
import io.netflix.titus.common.util.cache.Caches;
import io.netflix.titus.common.util.rx.InstrumentedEventLoop;
import io.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultAgentResourceCache implements AgentResourceCache {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentResourceCache.class);

    private static final int MAX_CACHE_SIZE = 10_000;
    private static final String METRIC_NAME_ROOT = "titusMaster.scheduler.agentResourceCache";
    private static final String CREATE_OR_UPDATE_IDLE = "createOrUpdateIdle";
    private static final String CREATE_OR_UPDATE_ACTIVE = "createOrUpdateActive";

    private final InstrumentedEventLoop eventLoop;
    private final Cache<String, AgentResourceCacheInstance> idleCache;
    private final Cache<String, AgentResourceCacheInstance> activeCache;

    @Inject
    public DefaultAgentResourceCache(Registry registry) {
        this(registry, Schedulers.computation());
    }

    public DefaultAgentResourceCache(Registry registry,
                                     Scheduler scheduler) {
        eventLoop = ObservableExt.createEventLoop(METRIC_NAME_ROOT, registry, scheduler);
        idleCache = Caches.instrumentedCacheWithMaxSize(MAX_CACHE_SIZE, METRIC_NAME_ROOT + ".idle", registry);
        activeCache = Caches.instrumentedCacheWithMaxSize(MAX_CACHE_SIZE, METRIC_NAME_ROOT + ".active", registry);
    }

    @Override
    public void createOrUpdateIdle(String hostname, Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> function) {
        eventLoop.schedule(CREATE_OR_UPDATE_IDLE, () -> {
            AgentResourceCacheInstance existingCacheInstance = idleCache.getIfPresent(hostname);
            AgentResourceCacheInstance newCacheInstance = function.apply(Optional.ofNullable(existingCacheInstance));
            if (!newCacheInstance.equals(existingCacheInstance)) {
                logger.info("Creating or updating hostname: {} with value: {}", hostname, newCacheInstance);
                idleCache.put(hostname, newCacheInstance);
            }
        });
    }

    @Override
    public void createOrUpdateActive(String hostname, Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> function) {
        eventLoop.schedule(CREATE_OR_UPDATE_ACTIVE, () -> {
            AgentResourceCacheInstance existingCacheInstance = activeCache.getIfPresent(hostname);
            AgentResourceCacheInstance newCacheInstance = function.apply(Optional.ofNullable(existingCacheInstance));
            if (!newCacheInstance.equals(existingCacheInstance)) {
                logger.info("Creating or updating hostname: {} with value: {}", hostname, newCacheInstance);
                activeCache.put(hostname, newCacheInstance);
            }
        });
    }

    @Override
    public Optional<AgentResourceCacheInstance> getIdle(String hostname) {
        return Optional.ofNullable(idleCache.getIfPresent(hostname));
    }

    @Override
    public Optional<AgentResourceCacheInstance> getActive(String hostname) {
        return Optional.ofNullable(activeCache.getIfPresent(hostname));
    }
}
