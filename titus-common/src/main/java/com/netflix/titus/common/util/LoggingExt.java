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

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoggingExt {

    private static final Logger logger = LoggerFactory.getLogger(LoggingExt.class);

    private LoggingExt() {
    }

    /**
     * Execute an action with logging its execution time when it completes or fails.
     */
    public static <T> T timed(String message, Supplier<T> fun) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        Logger tLogger;
        if (stackTrace.length > 2) {
            tLogger = LoggerFactory.getLogger(stackTrace[2].getClassName());
        } else {
            tLogger = logger;
        }
        long startTime = System.currentTimeMillis();
        try {
            T result = fun.get();
            tLogger.info("{} finished after {}[ms]", message, System.currentTimeMillis() - startTime);
            return result;
        } catch (Exception e) {
            tLogger.info("{} failed after {}[ms]", message, System.currentTimeMillis() - startTime);
            throw e;
        }
    }
}
