/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.store.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;

/**
 * To better decouple service layer model from the persistence layer, we provide serialization
 * specific annotations via Jackson mix-ins.
 */
public abstract class ApplicationSlaMixIn {

    @JsonCreator
    public ApplicationSlaMixIn(
            @JsonProperty("appName") String appName,
            @JsonProperty("tier") Tier tier,
            @JsonProperty("resourceDimension") ResourceDimension resourceDimension,
            @JsonProperty("instanceCount") int instanceCount) {
    }
}
