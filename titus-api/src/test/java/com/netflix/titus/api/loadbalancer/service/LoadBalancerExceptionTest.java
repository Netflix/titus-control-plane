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

package com.netflix.titus.api.loadbalancer.service;

import org.junit.Test;
import org.slf4j.event.Level;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link LoadBalancerException} class.
 */
public class LoadBalancerExceptionTest {
    @Test
    public void getDefaultLogLevelTest() {
        assertEquals(Level.ERROR, LoadBalancerException.getLogLevel(new Exception()));
    }

    @Test
    public void getDefaultLoadBalancerExceptionLogLevelTest() {
        assertEquals(
                Level.ERROR,
                LoadBalancerException.getLogLevel(
                        LoadBalancerException.jobMaxLoadBalancers("job-id", 1, 1)));
    }

    @Test
    public void getTaskGroupNotFoundLogLevelTest() {
        assertEquals(
                Level.DEBUG,
                LoadBalancerException.getLogLevel(
                        LoadBalancerException.targetGroupNotFound("target-group-id", new Exception())));
    }
}
