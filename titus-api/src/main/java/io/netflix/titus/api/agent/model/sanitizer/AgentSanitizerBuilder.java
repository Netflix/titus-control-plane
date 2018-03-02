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

package io.netflix.titus.api.agent.model.sanitizer;

import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.model.sanitizer.EntitySanitizerBuilder;

public class AgentSanitizerBuilder {

    public static final String AGENT_SANITIZER = "agent";

    private final EntitySanitizerBuilder sanitizerBuilder = EntitySanitizerBuilder.stdBuilder();

    public EntitySanitizer build() {
        return sanitizerBuilder.build();
    }
}
