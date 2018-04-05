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

package com.netflix.titus.master.loadbalancer.service;

import java.time.Instant;

import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.TargetState;
import com.netflix.titus.common.util.rx.batch.Batchable;
import com.netflix.titus.common.util.rx.batch.Priority;

class TargetStateBatchable implements Batchable<LoadBalancerTarget> {
    private final Priority priority;
    private final Instant timestamp;

    private final TargetState targetState;

    TargetStateBatchable(Priority priority, Instant timestamp, TargetState targetState) {
        this.priority = priority;
        this.timestamp = timestamp;
        this.targetState = targetState;
    }

    @Override
    public LoadBalancerTarget getIdentifier() {
        return targetState.getLoadBalancerTarget();
    }

    @Override
    public Priority getPriority() {
        return priority;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    String getLoadBalancerId() {
        return getIdentifier().getLoadBalancerId();
    }

    String getIpAddress() {
        return getIdentifier().getIpAddress();
    }

    LoadBalancerTarget.State getState() {
        return targetState.getState();
    }

    TargetState getTargetState() {
        return targetState;
    }

    @Override
    public boolean isEquivalent(Batchable<?> other) {
        if (!(other instanceof TargetStateBatchable)) {
            return false;
        }
        TargetStateBatchable o = (TargetStateBatchable) other;
        return targetState.equals(o.targetState);
    }
}
