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

package com.netflix.titus.testkit.model.agent;

import java.util.function.Function;

import com.netflix.titus.api.agent.model.AgentFunctions;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.common.util.time.Clocks;

/**
 * Test counterpart of {@link AgentFunctions}. Mutations here are less strict, this resulting in more compact test code.
 */
public final class AgentTestFunctions {

    private AgentTestFunctions() {
    }

    public static Function<AgentInstanceGroup, AgentInstanceGroup> inState(InstanceGroupLifecycleState state) {
        return AgentFunctions.inState(state, "No detail given", Clocks.system());
    }
}
