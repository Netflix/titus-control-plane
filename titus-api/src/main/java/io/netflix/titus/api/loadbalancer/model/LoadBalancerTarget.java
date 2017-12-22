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
    public enum State {Registered, Deregistered}

    private final JobLoadBalancer jobLoadBalancer;
    private final String ipAddress;

    /**
     * taskId is a descriptive field only, it is not part of the identity of this object
     */
    private final String taskId;

    public LoadBalancerTarget(JobLoadBalancer jobLoadBalancer, String taskId, String ipAddress) {
        this.jobLoadBalancer = jobLoadBalancer;
        this.ipAddress = ipAddress;
        this.taskId = taskId;
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

    @Override
    public String toString() {
        return "LoadBalancerTarget{" +
                "jobLoadBalancer=" + jobLoadBalancer +
                ", taskId='" + taskId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
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
