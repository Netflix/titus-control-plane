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

package com.netflix.titus.common.util.archaius2;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import reactor.core.publisher.Flux;

public final class Archaius2Ext {

    /**
     * Observe property value changes. Emits current value on subscriptions.
     */
    public static <T> Flux<T> watch(Property<T> property) {
        Flux<T> observer = Flux.create(emitter -> {
            Property.Subscription subscription = property.subscribe(emitter::next);
            emitter.onCancel(subscription::unsubscribe);
        });

        return Flux.just(property.get()).concatWith(observer);
    }

    /**
     * See {@link #watch(Property)}.
     */
    public static <T> Flux<T> watch(PropertyRepository propertyRepository, String key, Class<T> type) {
        return watch(propertyRepository.get(key, type));
    }
}
