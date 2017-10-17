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

package io.netflix.titus.common.util.code;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CodePointTracker} implementation that records all encounters in the metrics registry.
 * Additionally, each first occurrence of a code point/context is logged.
 */
public class SpectatorCodePointTracker extends CodePointTracker {

    private static final Logger logger = LoggerFactory.getLogger(SpectatorCodePointTracker.class);

    private final Registry registry;
    /* Visible for testing */ final ConcurrentMap<CodePoint, Counter> marked = new ConcurrentHashMap<>();

    public SpectatorCodePointTracker(Registry registry) {
        this.registry = registry;
    }

    protected void markReachable(CodePoint codePoint) {
        if (!marked.containsKey(codePoint)) {
            if (marked.putIfAbsent(codePoint, createCounter(codePoint)) == null) {
                markFirst(codePoint);
            }
        }
        marked.get(codePoint).increment();
    }

    private Counter createCounter(CodePoint codePoint) {
        return registry.counter(
                "titus.codePoint",
                "class", codePoint.getClassName(),
                "method", codePoint.getMethodName() + 'L' + codePoint.getLineNumber()
        );
    }

    private void markFirst(CodePoint codePoint) {
        logger.warn("Hit {}", codePoint);
    }
}
