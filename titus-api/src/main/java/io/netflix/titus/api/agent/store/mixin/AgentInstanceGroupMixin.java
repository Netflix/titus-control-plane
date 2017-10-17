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

package io.netflix.titus.api.agent.store.mixin;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;

public abstract class AgentInstanceGroupMixin {
    @JsonCreator
    public AgentInstanceGroupMixin(@JsonProperty("id") String id,
                                   @JsonProperty("instanceType") String instanceType,
                                   @JsonProperty("tier") Tier tier,
                                   @JsonProperty("resourceDimension") ResourceDimension resourceDimension,
                                   @JsonProperty("min") int min,
                                   @JsonProperty("desired") int desired,
                                   @JsonProperty("current") int current,
                                   @JsonProperty("max") int max,
                                   @JsonProperty("launchEnabled") boolean isLaunchEnabled,
                                   @JsonProperty("terminateEnabled") boolean isTerminateEnabled,
                                   @JsonProperty("autoScaleRule") AutoScaleRule autoScaleRule,
                                   @JsonProperty("lifecycleStatus") InstanceGroupLifecycleStatus lifecycleStatus,
                                   @JsonProperty("launchTimestamp") long launchTimestamp,
                                   @JsonProperty("attributes") Map<String, String> attributes) {
    }
}
