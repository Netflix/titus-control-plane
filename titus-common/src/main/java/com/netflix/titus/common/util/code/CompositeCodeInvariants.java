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

import java.util.List;

import static java.util.Arrays.asList;

public class CompositeCodeInvariants extends CodeInvariants {

    private final List<CodeInvariants> codeInvariants;

    public CompositeCodeInvariants(CodeInvariants... codeInvariants) {
        this(asList(codeInvariants));
    }

    public CompositeCodeInvariants(List<CodeInvariants> codeInvariants) {
        this.codeInvariants = codeInvariants;
    }

    @Override
    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        codeInvariants.forEach(i -> i.isTrue(condition, message, args));
        return this;
    }

    @Override
    public CodeInvariants notNull(Object value, String message, Object... args) {
        codeInvariants.forEach(i -> i.notNull(value, message, args));
        return this;
    }

    @Override
    public CodeInvariants inconsistent(String message, Object... args) {
        codeInvariants.forEach(i -> i.inconsistent(message, args));
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Exception e) {
        codeInvariants.forEach(i -> i.unexpectedError(message, e));
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Object... args) {
        codeInvariants.forEach(i -> i.unexpectedError(message, args));
        return this;
    }
}
