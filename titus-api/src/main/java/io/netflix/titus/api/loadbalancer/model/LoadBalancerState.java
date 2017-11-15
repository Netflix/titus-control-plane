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

package io.netflix.titus.api.loadbalancer.model;

public class LoadBalancerState {
    private final String loadBalancerId;
    private final JobLoadBalancer.State state;

    public LoadBalancerState(String loadBalancerId, JobLoadBalancer.State state) {
        this.loadBalancerId = loadBalancerId;
        this.state = state;
    }

    public String getLoadBalancerId() {
        return loadBalancerId;
    }

    public JobLoadBalancer.State getState() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LoadBalancerState that = (LoadBalancerState) o;

        if (!loadBalancerId.equals(that.loadBalancerId)) return false;
        return state == that.state;
    }

    @Override
    public int hashCode() {
        int result = loadBalancerId.hashCode();
        result = 31 * result + state.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LoadBalancerState{" +
                "loadBalancerId='" + loadBalancerId + '\'' +
                ", state=" + state +
                '}';
    }
}
