/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.scheduler;

import java.util.Collections;
import javax.inject.Singleton;

import com.netflix.fenzo.queues.tiered.TieredQueueSlas;
import rx.Observable;

@Singleton
public class NoOpTierSlaUpdater implements TierSlaUpdater {

    private final TieredQueueSlas empty = new TieredQueueSlas(Collections.emptyMap(), Collections.emptyMap());

    @Override
    public Observable<TieredQueueSlas> tieredQueueSlaUpdates() {
        return Observable.just(empty).concatWith(Observable.never());
    }
}
