/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatingNodeDataResolver implements NodeDataResolver {

    private final List<NodeDataResolver> delegates;

    public AggregatingNodeDataResolver(List<NodeDataResolver> delegates) {
        this.delegates = delegates;
    }

    @Override
    public Map<String, Node> resolve() {
        Map<String, Node> result = new HashMap<>();
        delegates.forEach(delegate -> result.putAll(delegate.resolve()));
        return result;
    }

    @Override
    public long getStalenessMs() {
        long max = 0;
        for (NodeDataResolver delegate : delegates) {
            max = Math.max(max, delegate.getStalenessMs());
        }
        return max;
    }
}
