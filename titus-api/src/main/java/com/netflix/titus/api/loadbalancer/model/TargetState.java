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

package com.netflix.titus.api.loadbalancer.model;

public class TargetState {
    private final LoadBalancerTarget loadBalancerTarget;
    private final LoadBalancerTarget.State state;

    public TargetState(LoadBalancerTarget loadBalancerTarget, LoadBalancerTarget.State state) {
        this.loadBalancerTarget = loadBalancerTarget;
        this.state = state;
    }

    public LoadBalancerTarget getLoadBalancerTarget() {
        return loadBalancerTarget;
    }

    public LoadBalancerTarget.State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "TargetState{" +
                "loadBalancerTarget=" + loadBalancerTarget +
                ", state=" + state +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TargetState that = (TargetState) o;

        if (!loadBalancerTarget.equals(that.loadBalancerTarget)) return false;
        return state == that.state;
    }

    @Override
    public int hashCode() {
        int result = loadBalancerTarget.hashCode();
        result = 31 * result + state.hashCode();
        return result;
    }
}
