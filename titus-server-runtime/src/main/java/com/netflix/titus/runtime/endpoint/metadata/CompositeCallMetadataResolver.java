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

package com.netflix.titus.runtime.endpoint.metadata;

import java.util.List;
import java.util.Optional;

public class CompositeCallMetadataResolver implements CallMetadataResolver {

    private final List<CallMetadataResolver> resolvers;

    public CompositeCallMetadataResolver(List<CallMetadataResolver> resolvers) {
        this.resolvers = resolvers;
    }

    @Override
    public Optional<CallMetadata> resolve() {
        for (CallMetadataResolver resolver : resolvers) {
            Optional<CallMetadata> next = resolver.resolve();
            if (next.isPresent()) {
                return next;
            }
        }
        return Optional.empty();
    }
}
