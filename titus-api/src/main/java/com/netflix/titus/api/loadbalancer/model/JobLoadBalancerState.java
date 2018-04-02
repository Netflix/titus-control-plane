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

import java.util.Map;
import java.util.Objects;

import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer.State;
import com.netflix.titus.common.util.tuple.Pair;

public class JobLoadBalancerState {
    private final JobLoadBalancer jobLoadBalancer;
    private final JobLoadBalancer.State state;

    public JobLoadBalancerState(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state) {
        this.jobLoadBalancer = jobLoadBalancer;
        this.state = state;
    }

    public JobLoadBalancer getJobLoadBalancer() {
        return jobLoadBalancer;
    }

    public JobLoadBalancer.State getState() {
        return state;
    }

    public String getJobId() {
        return jobLoadBalancer.getJobId();
    }

    public String getLoadBalancerId() {
        return jobLoadBalancer.getLoadBalancerId();
    }

    public boolean isStateAssociated() {
        return JobLoadBalancer.State.Associated.equals(state);
    }

    public boolean isStateDissociated() {
        return JobLoadBalancer.State.Dissociated.equals(state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JobLoadBalancerState)) {
            return false;
        }
        JobLoadBalancerState that = (JobLoadBalancerState) o;
        return Objects.equals(jobLoadBalancer, that.jobLoadBalancer) &&
                state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobLoadBalancer, state);
    }

    @Override
    public String toString() {
        return "JobLoadBalancerState{" +
                "jobLoadBalancer=" + jobLoadBalancer +
                ", state=" + state +
                '}';
    }

    public static JobLoadBalancerState from(Map.Entry<JobLoadBalancer, JobLoadBalancer.State> entry) {
        return new JobLoadBalancerState(entry.getKey(), entry.getValue());
    }

    public static JobLoadBalancerState from(Pair<JobLoadBalancer, JobLoadBalancer.State> entry) {
        return new JobLoadBalancerState(entry.getLeft(), entry.getRight());
    }
}
