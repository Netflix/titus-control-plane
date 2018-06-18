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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.time.Clocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes code invariant violations to a log file.
 */
public class LoggingCodeInvariants extends CodeInvariants {

    private static final Logger logger = LoggerFactory.getLogger(LoggingCodeInvariants.class);

    private static final long DROPPED_LOGS_INTERVAL_MS = 5_000;

    private static final CodeInvariants INSTANCE = new LoggingCodeInvariants(
            Limiters.createFixedIntervalTokenBucket(
                    "invariants", 1000, 1000, 100, 1, TimeUnit.SECONDS
            ),
            Clocks.system()
    );

    private final TokenBucket tokenBucket;
    private final Clock clock;
    private final AtomicLong dropped = new AtomicLong();
    private volatile long droppedReportTimestamp;

    public LoggingCodeInvariants(TokenBucket tokenBucket, Clock clock) {
        this.tokenBucket = tokenBucket;
        this.clock = clock;
    }

    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            inconsistent(message, args);
        }
        return this;
    }

    public CodeInvariants notNull(Object value, String message, Object... args) {
        if (value == null) {
            inconsistent(message, args);
        }
        return this;
    }

    public CodeInvariants inconsistent(String message, Object... args) {
        if (!canWrite()) {
            return this;
        }

        if (args.length == 0) {
            logger.warn(message);
        }

        try {
            logger.warn(String.format(message, args));
        } catch (Exception e) {
            String errorMessage = message + " (" + e.getMessage() + ')';
            logger.warn(errorMessage);
            logger.debug(errorMessage, e);
        }

        return this;
    }

    public CodeInvariants unexpectedError(String message, Exception e) {
        if (!canWrite()) {
            return this;
        }

        if (e == null || e.getMessage() == null) {
            logger.warn(message);
        } else {
            logger.warn("{}: {}", message, e.getMessage());
            logger.debug(message, e);
        }

        return this;
    }

    public CodeInvariants unexpectedError(String message, Object... args) {
        if (!canWrite()) {
            return this;
        }

        logger.warn(String.format(message, args));

        return this;
    }

    private boolean canWrite() {
        if (tokenBucket.tryTake()) {
            return true;
        }
        dropped.incrementAndGet();
        long dropReportDelayMs = clock.wallTime() - droppedReportTimestamp;
        if (dropReportDelayMs > DROPPED_LOGS_INTERVAL_MS) {
            long current = dropped.getAndSet(0);
            if (current > 0) {
                logger.warn("Dropped invariant violation logs: count={}", current);
                droppedReportTimestamp = clock.wallTime();
            }
        }
        return false;
    }

    public static CodeInvariants getDefault() {
        return INSTANCE;
    }
}
