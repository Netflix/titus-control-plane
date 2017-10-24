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

public class LoadBalancerTarget {
    private final JobLoadBalancer jobLoadBalancer;
    private final String taskId;
    private final String ipAddress;
    private final State state;

    public enum State {
        Registered,
        Deregistered
    }

    public LoadBalancerTarget(JobLoadBalancer jobLoadBalancer, String taskId, String ipAddress, State state) {
        this.jobLoadBalancer = jobLoadBalancer;
        this.taskId = taskId;
        this.ipAddress = ipAddress;
        this.state = state;
    }

    public JobLoadBalancer getJobLoadBalancer() {
        return jobLoadBalancer;
    }

    public String getLoadBalancerId() {
        return jobLoadBalancer.getLoadBalancerId();
    }

    public String getJobId() {
        return jobLoadBalancer.getJobId();
    }

    public String getTaskId() {
        return taskId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "LoadBalancerTarget{" +
                "jobLoadBalancer=" + jobLoadBalancer +
                ", taskId='" + taskId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", state=" + state +
                '}';
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

        if (!jobLoadBalancer.getLoadBalancerId().equals(that.jobLoadBalancer.getLoadBalancerId())) {
            return false;
        }
        return ipAddress.equals(that.ipAddress);
    }

    @Override
    public int hashCode() {
        int result = jobLoadBalancer.getLoadBalancerId().hashCode();
        result = 31 * result + ipAddress.hashCode();
        return result;
    }
}
