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

import java.time.DayOfWeek;
import java.util.EnumSet;

public enum Day {

    Monday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.MONDAY;
        }
    },
    Tuesday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.TUESDAY;
        }
    },
    Wednesday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.WEDNESDAY;
        }
    },
    Thursday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.THURSDAY;
        }
    },
    Friday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.FRIDAY;
        }
    },
    Saturday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.SATURDAY;
        }
    },
    Sunday() {
        @Override
        public DayOfWeek toDayOfWeek() {
            return DayOfWeek.SUNDAY;
        }
    };

    private static EnumSet<Day> WEEKDAYS = EnumSet.of(Monday, Tuesday, Wednesday, Thursday, Friday);
    private static EnumSet<Day> WEEKEND = EnumSet.of(Saturday, Sunday);

    public abstract DayOfWeek toDayOfWeek();

    public static EnumSet<Day> weekdays() {
        return WEEKDAYS;
    }

    public static EnumSet<Day> weekend() {
        return WEEKEND;
    }
}
