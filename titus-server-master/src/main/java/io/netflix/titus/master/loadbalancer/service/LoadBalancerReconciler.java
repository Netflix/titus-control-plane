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

package io.netflix.titus.master.loadbalancer.service;

import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import rx.Observable;

public interface LoadBalancerReconciler {
    /**
     * Stop reconciliation internal processes. Reconciliation is not guaranteed to continue after it is called.
     */
    void shutdown();

    /**
     * Periodically emit events for targets that need to be updated based on what the state they should be in.
     */
    Observable<TargetStateBatchable> events();

    /**
     * Mark a target to be ignored for a while, so reconciliation does not try to undo (or re-do) an update for it that
     * is currently in-flight. This is necessary since reconciliation often runs off of stale data (cached snapshots)
     * and there are propagation delays until updates can be detected by the reconciliation loop.
     *
     * @param target to be ignored
     * @param period to ignore it for
     */
    void activateCooldownFor(LoadBalancerTarget target, long period, TimeUnit unit);
}
