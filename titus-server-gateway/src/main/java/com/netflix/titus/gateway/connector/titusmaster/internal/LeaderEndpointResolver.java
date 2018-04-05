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

package com.netflix.titus.gateway.connector.titusmaster.internal;

import java.util.Optional;

import com.netflix.titus.common.network.http.EndpointResolver;
import com.netflix.titus.gateway.connector.titusmaster.Address;
import com.netflix.titus.gateway.connector.titusmaster.LeaderResolver;

public class LeaderEndpointResolver implements EndpointResolver {
    private final LeaderResolver leaderResolver;

    public LeaderEndpointResolver(LeaderResolver leaderResolver) {
        this.leaderResolver = leaderResolver;
    }

    @Override
    public String resolve() {
        Optional<Address> addressOptional = leaderResolver.resolve();
        if (addressOptional.isPresent()) {
            return addressOptional.get().toString();
        } else {
            throw new RuntimeException("No Leader Host Available");
        }
    }
}
