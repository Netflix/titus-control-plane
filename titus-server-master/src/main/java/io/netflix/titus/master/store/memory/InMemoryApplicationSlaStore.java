/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.store.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;

import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.master.store.ApplicationSlaStore;
import io.netflix.titus.master.store.exception.NotFoundException;
import rx.Observable;

@Singleton
public class InMemoryApplicationSlaStore implements ApplicationSlaStore {

    private final Map<String, ApplicationSLA> applicationSLAs = new ConcurrentHashMap<>();

    @Override
    public Observable<Void> create(ApplicationSLA applicationSLA) {
        return Observable.create(subscriber -> {
            applicationSLAs.put(applicationSLA.getAppName(), applicationSLA);
            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<ApplicationSLA> findAll() {
        return Observable.from(applicationSLAs.values());
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
            ApplicationSLA result = applicationSLAs.remove(applicationName);
            if (result == null) {
                subscriber.onError(new NotFoundException(ApplicationSLA.class, applicationName));
            } else {
                subscriber.onCompleted();
            }
        });
    }
}
