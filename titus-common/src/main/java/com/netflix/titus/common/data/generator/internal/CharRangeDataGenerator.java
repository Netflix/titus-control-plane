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

package com.netflix.titus.common.data.generator.internal;

import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.data.generator.DataGenerator;

public class CharRangeDataGenerator extends DataGenerator<Character> {
    private final char from;
    private final char to;
    private final Optional<Character> current;

    public CharRangeDataGenerator(char from, char to, char current) {
        Preconditions.checkArgument(from <= to, "Invalid value range: " + from + " > " + to);
        this.from = from;
        this.to = to;
        this.current = Optional.of(current);
    }

    @Override
    public DataGenerator<Character> apply() {
        if (current.get() == (to - 1)) {
            return (DataGenerator<Character>) EOS;
        }
        char c = current.get();
        c++;
        return new CharRangeDataGenerator(from, to, c);
    }

    @Override
    public Optional<Character> getOptionalValue() {
        return current;
    }
}
