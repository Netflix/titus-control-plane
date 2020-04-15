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

package com.netflix.titus.master.store.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.api.store.v2.exception.NotFoundException;
import rx.Observable;

@Singleton
public class InMemoryApplicationSlaStore implements ApplicationSlaStore {

    private final Map<String, ApplicationSLA> applicationSLAs = new ConcurrentHashMap<>();

    private final Multimap<String, ApplicationSLA> applicationSLAsBySchedulerName = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private final Object lock = new Object();

    @Override
    public Observable<Void> create(ApplicationSLA applicationSLA) {
        return Observable.create(subscriber -> {
            synchronized (lock) {
                applicationSLAs.put(applicationSLA.getAppName(), applicationSLA);
                applicationSLAsBySchedulerName.put(applicationSLA.getSchedulerName(), applicationSLA);
                subscriber.onCompleted();
            }
        });
    }

    @Override
    public Observable<ApplicationSLA> findAll() {
        return Observable.from(applicationSLAs.values());
    }

    @Override
    public Observable<ApplicationSLA> findBySchedulerName(String schedulerName) {
        return Observable.from(applicationSLAsBySchedulerName.get(schedulerName));
    }

    @Override
    public Observable<ApplicationSLA> findByName(String applicationName) {
        ApplicationSLA result = applicationSLAs.get(applicationName);
        if (result == null) {
            return Observable.error(new NotFoundException(ApplicationSLA.class, applicationName));
        }
        return Observable.just(result);
    }

    @Override
    public Observable<Void> remove(String applicationName) {
        return Observable.create(subscriber -> {
            synchronized (lock) {
                ApplicationSLA result = applicationSLAs.remove(applicationName);
                if (result == null) {
                    subscriber.onError(new NotFoundException(ApplicationSLA.class, applicationName));
                } else {
                    applicationSLAsBySchedulerName.remove(result.getSchedulerName(), result);
                    subscriber.onCompleted();
                }
            }
        });
    }
}
