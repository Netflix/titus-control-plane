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

package com.netflix.titus.api.store.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.api.store.v2.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Caching proxy for {@link ApplicationSlaStore}. It loads all data on startup in a blocking mode to fail fast,
 * in case there is any problem with the storage.
 */
@ProxyConfiguration(types = ProxyType.ActiveGuard)
public class ApplicationSlaStoreCache implements ApplicationSlaStore {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationSlaStoreCache.class);

    private final Object lock = new Object();
    private final ApplicationSlaStore delegate;
    private volatile ConcurrentMap<String, ApplicationSLA> cache;
    private volatile Multimap<String, ApplicationSLA> cacheBySchedulerName;

    public ApplicationSlaStoreCache(ApplicationSlaStore delegate) {
        this.delegate = delegate;
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Entering active mode");
        this.cache = loadCache(delegate);
        this.cacheBySchedulerName = loadSchedulerMapCache(this.cache);
        return Observable.empty();
    }

    @Override
    public Observable<Void> create(ApplicationSLA applicationSLA) {
        return delegate.create(applicationSLA).doOnCompleted(() ->
        {
            synchronized (lock) {
                ApplicationSLA previous = cache.put(applicationSLA.getAppName(), applicationSLA);
                if(previous != null){
                    cacheBySchedulerName.remove(previous.getSchedulerName(), previous);
                }
                cacheBySchedulerName.put(applicationSLA.getSchedulerName(), applicationSLA);
            }
        });
    }

    @Override
    public Observable<ApplicationSLA> findAll() {
        return Observable.create(subscriber -> {
            List<ApplicationSLA> snapshot = new ArrayList<>(cache.values());
            snapshot.forEach(subscriber::onNext);
            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<ApplicationSLA> findBySchedulerName(String schedulerName) {
        return Observable.create(subscriber -> {
            List<ApplicationSLA> snapshot = new ArrayList<>(cacheBySchedulerName.get(schedulerName));
            snapshot.forEach(subscriber::onNext);
            subscriber.onCompleted();
        });
    }

    public Observable<ApplicationSLA> findByName(String applicationName) {
        return Observable.create(subscriber -> {
            ApplicationSLA applicationSLA = cache.get(applicationName);
            if (applicationSLA != null) {
                subscriber.onNext(applicationSLA);
                subscriber.onCompleted();
            } else {
                subscriber.onError(new NotFoundException(ApplicationSLA.class, applicationName));
            }
        });
    }

    @Override
    public Observable<Void> remove(String applicationName) {
        return delegate.remove(applicationName).doOnCompleted(() -> {
            synchronized (lock) {
                ApplicationSLA result = cache.remove(applicationName);
                if (result != null) {
                    cacheBySchedulerName.remove(result.getSchedulerName(), result);
                }
            }
        });
    }

    private ConcurrentMap<String, ApplicationSLA> loadCache(ApplicationSlaStore delegate) {
        ConcurrentMap<String, ApplicationSLA> cache = new ConcurrentHashMap<>();
        delegate.findAll().doOnNext(a -> cache.put(a.getAppName(), a)).ignoreElements().toBlocking().firstOrDefault(null);
        return cache;
    }

    private Multimap<String, ApplicationSLA> loadSchedulerMapCache(Map<String, ApplicationSLA> starterCache) {
        Multimap<String, ApplicationSLA> schedulerMapCache = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        starterCache.values().forEach(applicationSLA -> schedulerMapCache.put(applicationSLA.getSchedulerName(), applicationSLA));
        return schedulerMapCache;
    }
}
