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

package com.netflix.titus.api.agent.loadbalancer.model.sanitizer;

import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LoadBalancerSanitizerBuilderTest {
    private final EntitySanitizer sanitizer = new LoadBalancerSanitizerBuilder().build();

    @Test
    public void testJobLoadBalancerValidation() throws Exception {
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer("Some JobId", "Some LoadBalancerId");
        assertThat(sanitizer.validate(jobLoadBalancer)).isEmpty();
    }

    @Test
    public void testEmptyJobIdValidation() throws Exception {
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer("", "Some LoadBalancerId");
        assertThat(sanitizer.validate(jobLoadBalancer)).hasSize(1);
    }

    @Test
    public void testEmptyLoadBalancerIdValidation() throws Exception {
        JobLoadBalancer jobLoadBalancer = new JobLoadBalancer("Some JobId", "");
        assertThat(sanitizer.validate(jobLoadBalancer)).hasSize(1);
    }
}
