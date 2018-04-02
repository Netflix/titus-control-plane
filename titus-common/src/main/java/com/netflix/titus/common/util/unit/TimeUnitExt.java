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

package com.netflix.titus.common.util.unit;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.common.util.tuple.Pair;

/**
 * Supplementary methods for time interval with unit parsing.
 */
public class TimeUnitExt {

    private static final Pattern INTERVAL_WITH_UNIT_RE = Pattern.compile("(\\d+)(ms|s|m|h|d)");

    public static Optional<Pair<Long, TimeUnit>> parse(String intervalWithUnit) {
        Matcher matcher = INTERVAL_WITH_UNIT_RE.matcher(intervalWithUnit);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        long interval = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);
        switch (unit) {
            case "ms":
                return Optional.of(Pair.of(interval, TimeUnit.MILLISECONDS));
            case "s":
                return Optional.of(Pair.of(interval, TimeUnit.SECONDS));
            case "m":
                return Optional.of(Pair.of(interval, TimeUnit.MINUTES));
            case "h":
                return Optional.of(Pair.of(interval, TimeUnit.HOURS));
            case "d":
                return Optional.of(Pair.of(interval, TimeUnit.DAYS));
        }
        return Optional.empty();
    }

    public static Optional<Long> toMillis(String intervalWithUnit) {
        return parse(intervalWithUnit).map(p -> p.getRight().toMillis(p.getLeft()));
    }
}
