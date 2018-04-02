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

package com.netflix.titus.common.network.http.internal;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.common.network.http.EndpointResolver;

public class RoundRobinEndpointResolver implements EndpointResolver {

    private List<String> endpoints;
    private volatile int position;

    public RoundRobinEndpointResolver(String endpoint) {
        this(Collections.singletonList(endpoint));
    }

    public RoundRobinEndpointResolver(List<String> endpoints) {
        this.endpoints = endpoints;
    }

    @Override
    public String resolve() {
        int index = getNextPosition();
        return endpoints.get(index);
    }

    private int getNextPosition() {
        if (endpoints.size() <= 1) {
            return 0;
        } else {
            int index = position;
            position++;
            if (position >= endpoints.size()) {
                position = 0;
            }
            return index;
        }
    }
}
