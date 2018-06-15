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

package com.netflix.titus.common.util;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.common.util.cache.MemoizedFunction;
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
                flags,
                e -> errorLogger.warn("Not valid regular expression value in '{}' property: {}", propertyName, e.getMessage())
        );
    }

    private static Function<String, Matcher> dynamicMatcherInternal(Supplier<String> regExpSource, int flags, Consumer<Throwable> onError) {
        Function<String, Pattern> compilePattern = new MemoizedFunction<>((patternString, lastGoodPattern) -> {
            try {
                return Pattern.compile(patternString, flags);
            } catch (Exception e) {
                onError.accept(e);
                // there is nothing that can be done if the first patternString is invalid
                return lastGoodPattern.orElseThrow(() -> new RuntimeException(e));
            }
        });

        return text -> compilePattern.apply(regExpSource.get()).matcher(text);
    }
}
