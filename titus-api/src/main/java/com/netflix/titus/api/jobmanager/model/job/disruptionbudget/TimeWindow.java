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

import java.util.List;
import java.util.Objects;

public class TimeWindow {

    private final List<Day> days;
    private final List<HourlyTimeWindow> hourlyTimeWindows;

    public TimeWindow(List<Day> days, List<HourlyTimeWindow> hourlyTimeWindows) {
        this.days = days;
        this.hourlyTimeWindows = hourlyTimeWindows;
    }

    public List<Day> getDays() {
        return days;
    }

    public List<HourlyTimeWindow> getHourlyTimeWindows() {
        return hourlyTimeWindows;
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
        TimeWindow that = (TimeWindow) o;
        return Objects.equals(days, that.days) &&
                Objects.equals(hourlyTimeWindows, that.hourlyTimeWindows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(days, hourlyTimeWindows);
    }

    @Override
    public String toString() {
        return "TimeWindow{" +
                "days=" + days +
                ", hourlyTimeWindows=" + hourlyTimeWindows +
                '}';
    }

    public static final class Builder {
        private List<Day> days;
        private List<HourlyTimeWindow> hourlyTimeWindows;

        private Builder() {
        }

        public Builder withDays(List<Day> days) {
            this.days = days;
            return this;
        }

        public Builder withHourlyTimeWindows(List<HourlyTimeWindow> hourlyTimeWindows) {
            this.hourlyTimeWindows = hourlyTimeWindows;
            return this;
        }

        public Builder but() {
            return newBuilder().withDays(days).withHourlyTimeWindows(hourlyTimeWindows);
        }

        public TimeWindow build() {
            return new TimeWindow(days, hourlyTimeWindows);
        }
    }
}
