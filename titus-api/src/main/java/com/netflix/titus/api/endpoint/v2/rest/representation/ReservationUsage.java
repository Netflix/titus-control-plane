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

package com.netflix.titus.api.endpoint.v2.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReservationUsage {

    private static final ReservationUsage NONE = new ReservationUsage(0, 0, 0, 0);

    private final double cpu;

    private final long memoryMB;

    private final long diskMB;

    private final long networkMbs;

    @JsonCreator
    public ReservationUsage(@JsonProperty("cpu") double cpu,
                            @JsonProperty("memoryMB") long memoryMB,
                            @JsonProperty("diskMB") long diskMB,
                            @JsonProperty("networkMbs") long networkMbs) {
        this.cpu = cpu;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.networkMbs = networkMbs;
    }

    public static ReservationUsage none() {
        return NONE;
    }

    public double getCpu() {
        return cpu;
    }

    public long getMemoryMB() {
        return memoryMB;
    }

    public long getDiskMB() {
        return diskMB;
    }

    public long getNetworkMbs() {
        return networkMbs;
    }
}