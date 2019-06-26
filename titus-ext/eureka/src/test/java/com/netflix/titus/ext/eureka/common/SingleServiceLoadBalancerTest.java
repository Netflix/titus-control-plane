/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.eureka.common;

import java.util.concurrent.TimeUnit;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.ext.eureka.EurekaServerStub;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.ext.eureka.EurekaGenerator.newInstanceInfo;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class SingleServiceLoadBalancerTest {

    private static final InstanceInfo INSTANCE_1 = newInstanceInfo("id1", "myservice", "1.0.0.1", InstanceStatus.UP);
    private static final InstanceInfo INSTANCE_2 = newInstanceInfo("id2", "myservice", "1.0.0.2", InstanceStatus.UP);

    private final TestClock testClock = Clocks.testWorldClock();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testClock);

    private final EurekaServerStub eurekaServer = new EurekaServerStub();

    @Before
    public void setUp() {
        eurekaServer.register(INSTANCE_1);
        eurekaServer.register(INSTANCE_2);
        eurekaServer.triggerCacheRefreshUpdate();
    }

    @Test
    public void testChooseWhenNoFailures() {
        SingleServiceLoadBalancer singleLB = newSingleLB();
        assertThat(asList(takeNextOrFail(singleLB), takeNextOrFail(singleLB))).contains(INSTANCE_1, INSTANCE_2);
    }

    @Test
    public void testChooseWhenFailure() {
        SingleServiceLoadBalancer singleLB = newSingleLB();

        InstanceInfo first = takeNextOrFail(singleLB);
        singleLB.recordFailure(first);

        InstanceInfo healthy = first == INSTANCE_1 ? INSTANCE_2 : INSTANCE_1;
        assertThat(asList(takeNextOrFail(singleLB), takeNextOrFail(singleLB))).contains(healthy, healthy);

        // Move time past quarantine period
        testClock.advanceTime(1, TimeUnit.HOURS);
        assertThat(asList(takeNextOrFail(singleLB), takeNextOrFail(singleLB))).contains(INSTANCE_1, INSTANCE_2);
    }

    @Test
    public void testAllBad() {
        SingleServiceLoadBalancer singleLB = newSingleLB();

        InstanceInfo first = takeNextOrFail(singleLB);
        InstanceInfo second = takeNextOrFail(singleLB);
        singleLB.recordFailure(first);
        singleLB.recordFailure(second);

        assertThat(singleLB.chooseNext()).isEmpty();

        // Move time past quarantine period
        testClock.advanceTime(1, TimeUnit.HOURS);
        assertThat(asList(takeNextOrFail(singleLB), takeNextOrFail(singleLB))).contains(INSTANCE_1, INSTANCE_2);
    }

    private SingleServiceLoadBalancer newSingleLB() {
        return new SingleServiceLoadBalancer(
                eurekaServer.getEurekaClient(),
                "myservice",
                false,
                titusRuntime
        );
    }

    private InstanceInfo takeNextOrFail(SingleServiceLoadBalancer singleLB) {
        return singleLB.chooseNext().orElseThrow(() -> new IllegalStateException("not found"));
    }
}