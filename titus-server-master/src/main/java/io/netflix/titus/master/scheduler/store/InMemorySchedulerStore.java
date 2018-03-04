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

package io.netflix.titus.master.scheduler.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

import io.netflix.titus.api.scheduler.model.SystemSelector;
import io.netflix.titus.api.scheduler.store.SchedulerStore;
import io.netflix.titus.common.util.rx.ObservableExt;
import rx.Completable;
import rx.Observable;

@Singleton
public class InMemorySchedulerStore implements SchedulerStore {

    private final ConcurrentMap<String, SystemSelector> systemSelectorsById = new ConcurrentHashMap<>();

    @Override
    public Observable<SystemSelector> retrieveSystemSelectors() {
        return ObservableExt.fromCallable(systemSelectorsById::values);
    }

    @Override
    public Completable storeSystemSelector(SystemSelector systemSelector) {
        return Completable.fromAction(() -> systemSelectorsById.put(systemSelector.getId(), systemSelector));
    }

    @Override
    public Completable deleteSystemSelector(String id) {
        return Completable.fromAction(() -> systemSelectorsById.remove(id));
    }
}
