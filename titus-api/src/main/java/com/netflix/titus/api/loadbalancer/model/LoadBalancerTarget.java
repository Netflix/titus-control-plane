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

import java.util.Objects;
import javax.validation.constraints.Size;

public class LoadBalancerTarget {
    public enum State {REGISTERED, DEREGISTERED}

    @Size(min = 1, max = 1024)
    private final String loadBalancerId;
    private final String ipAddress;

    /**
     * taskId is a descriptive field only, it is not part of the identity of this object
     */
    private final String taskId;

    public LoadBalancerTarget(String loadBalancerId, String taskId, String ipAddress) {
        this.loadBalancerId = loadBalancerId;
        this.ipAddress = ipAddress;
        this.taskId = taskId;
    }

    public String getLoadBalancerId() {
        return loadBalancerId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public LoadBalancerTargetState withState(State state) {
        return new LoadBalancerTargetState(this, state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoadBalancerTarget that = (LoadBalancerTarget) o;
        return loadBalancerId.equals(that.loadBalancerId) &&
                ipAddress.equals(that.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadBalancerId, ipAddress);
    }

    @Override
    public String toString() {
        return "LoadBalancerTarget{" +
                "loadBalancerId='" + loadBalancerId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", taskId='" + taskId + '\'' +
                '}';
    }

}
