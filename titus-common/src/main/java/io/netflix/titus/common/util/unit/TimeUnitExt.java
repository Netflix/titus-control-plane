package io.netflix.titus.common.util.unit;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netflix.titus.common.util.tuple.Pair;

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
