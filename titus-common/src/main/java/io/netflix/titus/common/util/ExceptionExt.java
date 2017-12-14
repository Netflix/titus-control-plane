/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 */
public class ExceptionExt {

    public interface RunnableWithExceptions {
        void run() throws Exception;
    }

    public static void silent(RunnableWithExceptions runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            // Ignore
        }
    }

    public static Optional<Throwable> doCatch(Runnable action) {
        try {
            action.run();
        } catch (Throwable e) {
            return Optional.of(e);
        }
        return Optional.empty();
    }

    public static RuntimeException rethrow(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        throw new UncheckedExceptionWrapper(e);
    }

    public static <T> T rethrow(Callable<T> func) {
        try {
            return func.call();
        } catch (Throwable e) {
            rethrow(e);
            return null;
        }
    }

    public static void rethrow(RunnableWithExceptions func) {
        try {
            func.run();
        } catch (Throwable e) {
            rethrow(e);
        }
    }

    public static <T> Optional<T> doTry(Callable<T> callable) {
        try {
            return Optional.ofNullable(callable.call());
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    public static <T> Optional<T> doTry(Supplier<T> callable) {
        try {
            return Optional.ofNullable(callable.get());
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    public static String toMessageChain(Throwable error) {
        StringBuilder sb = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            sb.append('(').append(current.getClass().getSimpleName()).append(')').append(' ').append(current.getMessage());
            if (current.getCause() != null) {
                sb.append(" -CAUSED BY-> ");
            }
            current = current.getCause();
        }
        return sb.toString();
    }

    public static class UncheckedExceptionWrapper extends RuntimeException {
        private UncheckedExceptionWrapper(Throwable cause) {
            super(cause);
        }
    }
}
