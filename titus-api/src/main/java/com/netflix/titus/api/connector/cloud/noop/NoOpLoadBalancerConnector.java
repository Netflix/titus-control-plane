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

package com.netflix.titus.api.connector.cloud.noop;

import java.util.Collections;
import java.util.Set;

import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import rx.Completable;
import rx.Single;

/**
 * Test helper class that intentionally does nothing on register/deregister and returns success.
 */
public class NoOpLoadBalancerConnector implements LoadBalancerConnector {
    @Override
    public Completable registerAll(String loadBalancerId, Set<String> ipAddresses) {
        return Completable.complete();
    }

    @Override
    public Completable deregisterAll(String loadBalancerId, Set<String> ipAddresses) {
        return Completable.complete();
    }

    @Override
    public Completable isValid(String loadBalancerId) {
        return Completable.complete();
    }

    @Override
    public Single<Set<String>> getRegisteredIps(String loadBalancerId) {
        return Single.just(Collections.emptySet());
    }
}
