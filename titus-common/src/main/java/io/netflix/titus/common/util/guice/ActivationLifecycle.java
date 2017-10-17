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

package io.netflix.titus.common.util.guice;

import java.util.List;

import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.Deactivator;
import io.netflix.titus.common.util.tuple.Pair;

/**
 * Service activation/deactivation lifecycle handler. Use {@link Activator} and {@link Deactivator} annotations
 * to instrument your services.
 */
public interface ActivationLifecycle {

    /**
     * Returns true if a given service instance is active.
     */
    <T> boolean isActive(T instance);

    /**
     * Activate all services.
     */
    void activate();

    /**
     * Deactivate all services.
     */
    void deactivate();

    /**
     * Returns total activation time or -1 if none-of the services is activated yet or activation is in progress.
     */
    long getActivationTimeMs();

    /**
     * Returns service activation time or -1 if a service is not activated yet or its activation is in progress.
     * List elements are ordered according to the service activation order.
     */
    List<Pair<String, Long>> getServiceActionTimesMs();
}
