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

package io.netflix.titus.api.jobmanager.store.mixin;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.SecurityProfile;

public abstract class ContainerMixin {
    @JsonCreator
    public ContainerMixin(@JsonProperty("containerResources") ContainerResources containerResources,
                          @JsonProperty("securityProfile") SecurityProfile securityProfile,
                          @JsonProperty("image") Image image,
                          @JsonProperty("attributes") Map<String, String> attributes,
                          @JsonProperty("entryPoint") List<String> entryPoint,
                          @JsonProperty("command") List<String> command,
                          @JsonProperty("env") Map<String, String> env,
                          @JsonProperty("softConstraints") Map<String, String> softConstraints,
                          @JsonProperty("hardConstraints") Map<String, String> hardConstraints) {
    }
}
