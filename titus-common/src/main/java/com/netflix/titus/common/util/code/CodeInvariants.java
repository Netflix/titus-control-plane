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

    /**
     * Registers an expectation that <code>condition</code> is true.
     * @param condition A boolean that is expected to be true.
     * @param message For when <code>condition</code> is actually false, a
     * <a href="https://docs.oracle.com/javase/10/docs/api/java/util/Formatter.html#syntax">format string</a>
     * which should include information to help debug why <code>condition</code>
     * is false.
     * @param args Arguments to the format string.
     * @return <code>this</code>
     */
    public abstract CodeInvariants isTrue(boolean condition, String message, Object... args);

    /**
     * Registers an expectation that <code>value</code> is not null.
     * @param value A value that is expected to be non-null.
     * @param message For when <code>value</code> is actually null, a
     * <a href="https://docs.oracle.com/javase/10/docs/api/java/util/Formatter.html#syntax">format string</a>
     * which should include information to help debug why <code>value</code> is
     * null.
     * @param args Arguments to the format string.
     * @return <code>this</code>
     */
    public abstract CodeInvariants notNull(Object value, String message, Object... args);

    /**
     * Registers an inconsistency.
     * @param message A <a href="https://docs.oracle.com/javase/10/docs/api/java/util/Formatter.html#syntax">format string</a>
     * which should include information to help debug the cause of the inconsistency.
     * @param args Arguments to the format string.
     * @return <code>this</code>
     */
    public abstract CodeInvariants inconsistent(String message, Object... args);

    /**
     * Registers that an unexpected error has occurred.
     * @param message A message string which should include information to help
     * debug the cause of the unexpected error.
     * @param e An exception object, typically the unexpected error that occurred.
     * @return <code>this</code>
     */
    public abstract CodeInvariants unexpectedError(String message, Exception e);

    /**
     * Registers that an unexpected error has occurred.
     * @param message A <a href="https://docs.oracle.com/javase/10/docs/api/java/util/Formatter.html#syntax">format string</a>
     * which should include information to help debug the cause of the unexpected
     * error.
     * @param args Arguments to the format string.
     * @return <code>this</code>
     */
    public abstract CodeInvariants unexpectedError(String message, Object... args);
}
