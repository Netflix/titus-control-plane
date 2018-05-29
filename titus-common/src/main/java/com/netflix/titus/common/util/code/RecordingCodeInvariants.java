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

package com.netflix.titus.common.util.code;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.netflix.titus.common.util.ExceptionExt.doTry;

public class RecordingCodeInvariants extends CodeInvariants {

    private final List<String> violations = new CopyOnWriteArrayList<>();

    public List<String> getViolations() {
        return new ArrayList<>(violations);
    }

    @Override
    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            violations.add(doTry(() -> String.format(message, args)).orElse("Invalid pattern or arguments: " + message));
        }
        return this;
    }

    @Override
    public CodeInvariants notNull(Object value, String message, Object... args) {
        return isTrue(value != null, message, args);
    }

    @Override
    public CodeInvariants inconsistent(String message, Object... args) {
        return isTrue(false, message, args);
    }

    @Override
    public CodeInvariants unexpectedError(String message, Exception e) {
        return isTrue(false, message);
    }

    @Override
    public CodeInvariants unexpectedError(String message, Object... args) {
        return isTrue(false, message, args);
    }
}
