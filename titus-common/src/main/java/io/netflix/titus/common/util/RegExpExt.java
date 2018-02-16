package io.netflix.titus.common.util;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;

public final class RegExpExt {

    private RegExpExt() {
    }

    /**
     * Create a {@link Matcher} factory function, with dynamic update of the regular expression pattern.
     * This class is primarily to be used with patterns setup via dynamic configuration.
     */
    public static Function<String, Matcher> dynamicMatcher(Supplier<String> regExpSource, int flags, Consumer<Throwable> onError) {
        return dynamicMatcherInternal(regExpSource, flags, onError);
    }

    /**
     * A convenience wrapper for {@link #dynamicMatcher(Supplier, int, Consumer)} which automatically logs errors to the provided logger.
     */
    public static Function<String, Matcher> dynamicMatcher(Supplier<String> regExpSource, String propertyName, int flags, Logger errorLogger) {
        return dynamicMatcherInternal(
                regExpSource,
                flags, e -> errorLogger.warn("Not valid regular expression value in '{}' property: {}", propertyName, e.getMessage())
        );
    }

    private static Function<String, Matcher> dynamicMatcherInternal(Supplier<String> regExpSource, int flags, Consumer<Throwable> onError) {
        String patternString = regExpSource.get();
        AtomicReference<Pair<String, Pattern>> lastGoodPattern = new AtomicReference<>(Pair.of(patternString, Pattern.compile(patternString, flags)));
        return text -> {
            Pair<String, Pattern> current = lastGoodPattern.get();

            String currentPatternString = current.getLeft();
            String newPatternString = regExpSource.get();

            if (!currentPatternString.equals(newPatternString)) {
                try {
                    Pattern newPattern = Pattern.compile(newPatternString, flags);
                    lastGoodPattern.set(Pair.of(newPatternString, newPattern));
                    return newPattern.matcher(text);
                } catch (Exception e) {
                    onError.accept(e);
                }
            }

            return current.getRight().matcher(text);
        };
    }
}
