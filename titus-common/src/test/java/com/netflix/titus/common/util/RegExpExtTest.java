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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RegExpExtTest {

    @Test
    public void testDynamicMatcher() {
        String message = "hello\nend";
        boolean matches = RegExpExt.dynamicMatcher(() -> ".*hello.*", Pattern.DOTALL, e -> {
            throw new IllegalStateException(e);
        }).apply(message).matches();
        assertThat(matches).isTrue();
    }

    @Test
    public void testDynamicModifications() {
        String message = "hello\nend";
        AtomicReference<String> config = new AtomicReference<>(".*hello.*");
        Function<String, Matcher> matcher = RegExpExt.dynamicMatcher(() -> config.get(), Pattern.DOTALL, e -> {
            throw new IllegalStateException(e);
        });
        assertThat(matcher.apply(message).matches()).isTrue();

        config.set(".*foobar.*");
        assertThat(matcher.apply(message).matches()).isFalse();
    }
}