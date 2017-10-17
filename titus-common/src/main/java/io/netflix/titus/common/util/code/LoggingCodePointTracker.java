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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple {@link CodePointTracker} implementation that logs all encounters into the log file.
 */
public class LoggingCodePointTracker extends CodePointTracker {

    private static final Logger logger = LoggerFactory.getLogger(LoggingCodePointTracker.class);

    @Override
    protected void markReachable(CodePoint codePoint) {
        logger.warn("Hit {}", codePoint);
    }
}
