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

/**
 * Association between a Job and a LoadBalancer
 */
public class JobLoadBalancer {
    private final String jobId;
    private final String loadBalancerId;

    public JobLoadBalancer(String jobId, String loadBalancerId) {
        this.jobId = jobId;
        this.loadBalancerId = loadBalancerId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getLoadBalancerId() {
        return loadBalancerId;
    }

    @Override
    public String toString() {
        return "JobLoadBalancer{" +
                "jobId='" + jobId + '\'' +
                ", loadBalancerId='" + loadBalancerId + '\'' +
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

        JobLoadBalancer that = (JobLoadBalancer) o;

        if (!jobId.equals(that.jobId)) {
            return false;
        }
        return loadBalancerId.equals(that.loadBalancerId);
    }

    @Override
    public int hashCode() {
        int result = jobId.hashCode();
        result = 31 * result + loadBalancerId.hashCode();
        return result;
    }
}
