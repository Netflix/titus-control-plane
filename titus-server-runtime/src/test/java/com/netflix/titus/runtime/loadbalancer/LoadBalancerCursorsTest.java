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
package com.netflix.titus.runtime.loadbalancer;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class LoadBalancerCursorsTest {
    private List<JobLoadBalancer> loadBalancerList;

    @Before
    public void setUp() {
        loadBalancerList = Stream.of(
                new JobLoadBalancer("job1", "lb1"),
                new JobLoadBalancer("job3", "lb3"),
                new JobLoadBalancer("job2", "lb2"),
                new JobLoadBalancer("job4", "lb4")
        ).sorted(LoadBalancerCursors.loadBalancerComparator()).collect(Collectors.toList());
    }


    @Test
    public void checkValidCursor() {
        final String cursor = LoadBalancerCursors.newCursorFrom(new JobLoadBalancer("job3", "lb3"));
        final Optional<Integer> loadBalancerIndex = LoadBalancerCursors.loadBalancerIndexOf(loadBalancerList, cursor);
        assertThat(loadBalancerIndex.isPresent()).isTrue();
        assertThat(loadBalancerIndex.get()).isEqualTo(2);
    }


    @Test
    public void checkMissingCursor() {
        final String cursor = LoadBalancerCursors.newCursorFrom(new JobLoadBalancer("job30", "lb3"));
        final Optional<Integer> loadBalancerIndex = LoadBalancerCursors.loadBalancerIndexOf(loadBalancerList, cursor);
        assertThat(loadBalancerIndex.isPresent()).isTrue();
        assertThat(loadBalancerIndex.get()).isEqualTo(1);
    }


    @Test
    public void checkFirstMissingCursorElement() {
        final String cursor = LoadBalancerCursors.newCursorFrom(new JobLoadBalancer("job1", "lb0"));
        final Optional<Integer> loadBalancerIndex = LoadBalancerCursors.loadBalancerIndexOf(loadBalancerList, cursor);
        assertThat(loadBalancerIndex.isPresent()).isTrue();
        assertThat(loadBalancerIndex.get()).isEqualTo(-1);
    }


}
