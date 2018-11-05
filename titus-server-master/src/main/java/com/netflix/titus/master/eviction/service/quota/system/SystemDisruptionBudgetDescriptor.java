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

package com.netflix.titus.master.eviction.service.quota.system;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;

public class SystemDisruptionBudgetDescriptor {

    private final long refillRatePerSecond;
    private final long capacity;
    private final List<TimeWindow> timeWindows;

    @JsonCreator
    public SystemDisruptionBudgetDescriptor(@JsonProperty("refillRatePerSecond") long refillRatePerSecond,
                                            @JsonProperty("capacity") long capacity,
                                            @JsonProperty("timeWindows") List<TimeWindow> timeWindows) {
        this.refillRatePerSecond = refillRatePerSecond;
        this.capacity = capacity;
        this.timeWindows = timeWindows;
    }

    public long getRefillRatePerSecond() {
        return refillRatePerSecond;
    }

    public long getCapacity() {
        return capacity;
    }

    public List<TimeWindow> getTimeWindows() {
        return timeWindows;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemDisruptionBudgetDescriptor that = (SystemDisruptionBudgetDescriptor) o;
        return refillRatePerSecond == that.refillRatePerSecond &&
                capacity == that.capacity &&
                Objects.equals(timeWindows, that.timeWindows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(refillRatePerSecond, capacity, timeWindows);
    }

    @Override
    public String toString() {
        return "SystemDisruptionBudgetDescriptor{" +
                "refillRatePerSecond=" + refillRatePerSecond +
                ", capacity=" + capacity +
                ", timeWindows=" + timeWindows +
                '}';
    }
}
