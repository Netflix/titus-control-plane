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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import rx.Completable;

/**
 * Test helper class that intentionally does nothing on register/deregister and returns success.
 */
public class NoOpLoadBalancerConnector implements LoadBalancerConnector {
    private static Logger logger = LoggerFactory.getLogger(NoOpLoadBalancerConnector.class);
    @Override
    public Completable registerAll(String loadBalancerId, Set<String> ipAddresses) {
        return Completable.complete();
    }

    @Override
    public Completable deregisterAll(String loadBalancerId, Set<String> ipAddresses) {
        return Completable.complete();
    }

    @Override
    public Completable isValid(String loadBalancerId) { return Completable.complete(); }
}
