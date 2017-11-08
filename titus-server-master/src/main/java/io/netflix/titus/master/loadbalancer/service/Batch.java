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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;

class Batch {

    private final Group toRegister;
    private final Group toDeregister;

    Batch(Map<LoadBalancerTarget, State> batch) {
        final Map<State, List<LoadBalancerTarget>> groupedByState = batch.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                ));
        toRegister = new Group(groupedByState.getOrDefault(State.Registered, Collections.emptyList()));
        toDeregister = new Group(groupedByState.getOrDefault(State.Deregistered, Collections.emptyList()));

    }

    public Group getToRegister() {
        return toRegister;
    }

    public Group getToDeregister() {
        return toDeregister;
    }

    @Override
    public String toString() {
        return "Batch{" +
                "toRegister=" + toRegister +
                ", toDeregister=" + toDeregister +
                '}';
    }

    class Group {
        private final Set<LoadBalancerTarget> entries;

        Group(List<LoadBalancerTarget> items) {
            // reverse so we keep the last seen
            entries = new HashSet<>(Lists.reverse(items));
        }

        int size() {
            return entries.size();
        }

        Map<String, Set<LoadBalancerTarget>> byLoadBalancerId() {
            return entries.stream().collect(Collectors.groupingBy(LoadBalancerTarget::getLoadBalancerId))
                    .entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new HashSet<>(entry.getValue())
                    ));
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }

}
