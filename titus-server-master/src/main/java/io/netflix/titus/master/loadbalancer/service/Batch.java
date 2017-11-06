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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;

class Batch {
    private final Map<LoadBalancerTarget.State, List<LoadBalancerTarget>> groupedBy;

    Batch(Map<LoadBalancerTarget, LoadBalancerTarget.State> batch) {
        groupedBy = batch.entrySet().stream().collect(
                Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
    }

    List<LoadBalancerTarget> getStateRegister() {
        return groupedBy.getOrDefault(LoadBalancerTarget.State.Registered, Collections.emptyList());
    }

    List<LoadBalancerTarget> getStateDeregister() {
        return groupedBy.getOrDefault(LoadBalancerTarget.State.Deregistered, Collections.emptyList());
    }

    @Override
    public String toString() {
        return "Batch{" +
                "groupedBy=" + groupedBy +
                '}';
    }
}
