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

/**
 * {@link CodeInvariants} registers code invariant violations in functions or entities, which are usually not fatal
 * (system self-corrects itself), but are important to track and fix.
 */
public abstract class CodeInvariants {

    public abstract CodeInvariants isTrue(boolean condition, String message, Object... args);

    public abstract CodeInvariants notNull(Object value, String message, Object... args);

    public abstract CodeInvariants inconsistent(String message, Object... args);

    public abstract CodeInvariants unexpectedError(String message, Exception e);

    public abstract CodeInvariants unexpectedError(String message, Object... args);

    public static CodeInvariants codeInvariants() {
        return LoggingCodeInvariants.INSTANCE;
    }
}
