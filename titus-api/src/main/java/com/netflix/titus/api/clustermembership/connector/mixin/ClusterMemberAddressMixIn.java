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

package com.netflix.titus.api.clustermembership.connector.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class ClusterMemberAddressMixIn {

    @JsonCreator
    public ClusterMemberAddressMixIn(@JsonProperty("ipAddress") String ipAddress,
                                     @JsonProperty("portNumber") int portNumber,
                                     @JsonProperty("protocol") String protocol,
                                     @JsonProperty("secure") boolean secure,
                                     @JsonProperty("description") String description) {
    }
}
