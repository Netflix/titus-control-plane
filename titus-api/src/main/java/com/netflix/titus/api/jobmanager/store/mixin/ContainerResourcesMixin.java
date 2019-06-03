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

package com.netflix.titus.api.jobmanager.store.mixin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.EfsMount;

public abstract class ContainerResourcesMixin {
    @JsonCreator
    public ContainerResourcesMixin(@JsonProperty("cpu") double cpu,
                                   @JsonProperty("gpu") int gpu,
                                   @JsonProperty("memoryMB") int memoryMB,
                                   @JsonProperty("diskMB") int diskMB,
                                   @JsonProperty("networkMbps") int networkMbps,
                                   @JsonProperty("efsMounts") List<EfsMount> efsMounts,
                                   @JsonProperty("allocateIP") boolean allocateIP,
                                   @JsonProperty("shmMB") int shmMB,
                                   @JsonProperty("signedIpAddressAllocations")List<SignedIpAddressAllocation> signedIpAddressAllocations) {
    }
}
