/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.client;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.util.RegExpExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PodCreateErrorToResultCodeResolver {

    private static final Logger logger = LoggerFactory.getLogger(PodCreateErrorToResultCodeResolver.class);

    private final Function<String, Matcher> invalidPodMatcherFactory;
    private final Function<String, Matcher> transientSystemErrorMatcherFactory;

    PodCreateErrorToResultCodeResolver(DirectKubeConfiguration configuration) {
        this.invalidPodMatcherFactory = RegExpExt.dynamicMatcher(configuration::getInvalidPodMessagePattern, "invalidPodMessagePattern", Pattern.DOTALL, logger);
        this.transientSystemErrorMatcherFactory = RegExpExt.dynamicMatcher(configuration::getTransientSystemErrorMessagePattern, "transientSystemErrorMessagePattern", Pattern.DOTALL, logger);
    }

    String resolveReasonCode(Throwable cause) {
        try {
            if (cause.getMessage() != null) {
                return resolveReasonCodeFromMessage(cause.getMessage());
            }
            return TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
        } catch (Exception e) {
            return TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
        }
    }

    private String resolveReasonCodeFromMessage(String message) {
        if (invalidPodMatcherFactory.apply(message).matches()) {
            return TaskStatus.REASON_INVALID_REQUEST;
        }
        if (transientSystemErrorMatcherFactory.apply(message).matches()) {
            return TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR;
        }
        return TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
    }
}
