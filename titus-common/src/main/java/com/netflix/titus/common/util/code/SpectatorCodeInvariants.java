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

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public class SpectatorCodeInvariants extends CodeInvariants {

    private final Counter violations;

    public SpectatorCodeInvariants(Id rootId, Registry registry) {
        this.violations = registry.counter(rootId);
    }

    @Override
    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            violations.increment();
        }
        return this;
    }

    @Override
    public CodeInvariants notNull(Object value, String message, Object... args) {
        if (value == null) {
            violations.increment();
        }
        return this;
    }

    @Override
    public CodeInvariants inconsistent(String message, Object... args) {
        violations.increment();
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Exception e) {
        violations.increment();
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Object... args) {
        violations.increment();
        return this;
    }
}
