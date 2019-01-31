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

import javax.validation.constraints.Size;

/**
 * Association between a Job and a LoadBalancer
 */
public class JobLoadBalancer implements Comparable<JobLoadBalancer> {
    public enum State {Associated, Dissociated}

    @Size(min = 1, max = 50)
    private final String jobId;
    @Size(min = 1, max = 1024)
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

    @Override
    public int compareTo(JobLoadBalancer o) {
        if (!jobId.equals(o.getJobId())) {
            return jobId.compareTo(o.getJobId());
        }
        return loadBalancerId.compareTo(o.getLoadBalancerId());
    }

    /**
     * {@link java.util.function.BiPredicate} that compares <tt>loadBalancerIds</tt>.
     */
    public static boolean byLoadBalancerId(JobLoadBalancer one, JobLoadBalancer other) {
        return one.loadBalancerId.equals(other.loadBalancerId);
    }
}
