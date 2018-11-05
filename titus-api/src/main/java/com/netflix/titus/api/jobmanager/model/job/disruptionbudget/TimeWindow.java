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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

public class TimeWindow {

    public static String DEFAULT_TIME_ZONE = "UTC";

    private static final TimeWindow EMPTY = new TimeWindow(Collections.emptyList(), Collections.emptyList(), DEFAULT_TIME_ZONE);

    private final List<Day> days;
    private final List<HourlyTimeWindow> hourlyTimeWindows;
    private final String timeZone;

    public TimeWindow(List<Day> days, List<HourlyTimeWindow> hourlyTimeWindows, String timeZone) {
        this.days = days;
        this.hourlyTimeWindows = hourlyTimeWindows;
        this.timeZone = timeZone;
    }

    public List<Day> getDays() {
        return days;
    }

    public List<HourlyTimeWindow> getHourlyTimeWindows() {
        return hourlyTimeWindows;
    }

    public String getTimeZone() {
        return timeZone;
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
                Objects.equals(hourlyTimeWindows, that.hourlyTimeWindows) &&
                Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(days, hourlyTimeWindows, timeZone);
    }

    @Override
    public String toString() {
        return "TimeWindow{" +
                "days=" + days +
                ", hourlyTimeWindows=" + hourlyTimeWindows +
                ", timeZone='" + timeZone + '\'' +
                '}';
    }

    public static TimeWindow empty() {
        return EMPTY;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private List<Day> days;
        private List<HourlyTimeWindow> hourlyTimeWindows;
        private String timeZone;

        private Builder() {
        }

        public Builder withDays(Day... days) {
            getOrCreateMutableDaysList().addAll(Arrays.asList(days));
            return this;
        }

        public Builder withDays(Collection<Day> days) {
            this.days = new ArrayList<>(days);
            return this;
        }

        public Builder withHourlyTimeWindows(List<HourlyTimeWindow> hourlyTimeWindows) {
            this.hourlyTimeWindows = hourlyTimeWindows;
            return this;
        }

        public Builder withwithHourlyTimeWindows(int... startEndHours) {
            Preconditions.checkArgument(startEndHours.length % 2 == 0, "Expected pairs of start/end hours");
            getOrCreateMutableHoursList();

            for (int i = 0; i < startEndHours.length; i += 2) {
                this.hourlyTimeWindows.add(HourlyTimeWindow.newRange(startEndHours[i], startEndHours[i + 1]));
            }

            return this;
        }

        public Builder withTimeZone(String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder but() {
            return newBuilder().withDays(days).withHourlyTimeWindows(hourlyTimeWindows);
        }

        public TimeWindow build() {
            return new TimeWindow(days, hourlyTimeWindows, timeZone == null ? DEFAULT_TIME_ZONE : timeZone);
        }

        private List<Day> getOrCreateMutableDaysList() {
            if (this.days == null) {
                this.days = new ArrayList<>();
            } else if (!(this.days instanceof ArrayList)) {
                this.days = new ArrayList<>(this.days);
            }
            return days;
        }

        private List<HourlyTimeWindow> getOrCreateMutableHoursList() {
            if (this.hourlyTimeWindows == null) {
                this.hourlyTimeWindows = new ArrayList<>();
            } else if (!(this.hourlyTimeWindows instanceof ArrayList)) {
                this.hourlyTimeWindows = new ArrayList<>(this.hourlyTimeWindows);
            }
            return hourlyTimeWindows;
        }
    }
}
