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

package com.netflix.titus.api.jobmanager.model.job.disruptionbudget;

import java.util.Objects;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassInvariant;

@ClassInvariant(condition = "startHour < endHour", message = "'startHour'(#{startHour}) must be < 'endHour'(#{endHour})")
public class HourlyTimeWindow {

    @Min(0)
    @Max(24)
    private final int startHour;

    @Min(0)
    @Max(24)
    private final int endHour;

    public HourlyTimeWindow(int startHour, int endHour) {
        this.startHour = startHour;
        this.endHour = endHour;
    }

    public int getStartHour() {
        return startHour;
    }

    public int getEndHour() {
        return endHour;
    }

    public static HourlyTimeWindow newRange(int start, int end) {
        return new HourlyTimeWindow(start, end);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HourlyTimeWindow that = (HourlyTimeWindow) o;
        return startHour == that.startHour &&
                endHour == that.endHour;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startHour, endHour);
    }

    @Override
    public String toString() {
        return "HourlyTimeWindow{" +
                "startHour=" + startHour +
                ", endHour=" + endHour +
                '}';
    }

    public static final class Builder {
        private int startHour;
        private int endHour;

        private Builder() {
        }

        public Builder withStartHour(int startHour) {
            this.startHour = startHour;
            return this;
        }

        public Builder withEndHour(int endHour) {
            this.endHour = endHour;
            return this;
        }

        public Builder but() {
            return newBuilder().withStartHour(startHour).withEndHour(endHour);
        }

        public HourlyTimeWindow build() {
            return new HourlyTimeWindow(startHour, endHour);
        }
    }
}
