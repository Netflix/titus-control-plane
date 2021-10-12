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

package com.netflix.titus.master.kubernetes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.unit.TimeUnitExt;

import static com.netflix.titus.api.jobmanager.JobAttributes.TITUS_PARAMETER_ATTRIBUTE_PREFIX;

public class PerformanceToolUtil {

    private static final Pattern TASK_STATE_RULES_RE = Pattern.compile("(launched|startInitiated|started|killInitiated)\\s*:\\s*delay=(\\d+(ms|s|m|h|d))");

    // Mock VK titus job parameters
    static final String MOCK_VK_PROPERTY_PREFIX = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "mockVK.";
    static final String MOCK_VK_PROPERTY_PREPARE_TIME = MOCK_VK_PROPERTY_PREFIX + "prepareTime";
    static final String MOCK_VK_PROPERTY_RUN_TIME = MOCK_VK_PROPERTY_PREFIX + "runTime";
    static final String MOCK_VK_PROPERTY_KILL_TIME = MOCK_VK_PROPERTY_PREFIX + "killTime";

    static final String PREPARE_TIME = "github.com.netflix.titus.executor/prepareTime";
    static final String RUN_TIME = "github.com.netflix.titus.executor/runTime";
    static final String KILL_TIME = "github.com.netflix.titus.executor/killTime";

    public static Map<String, String> toAnnotations(Job job) {
        Map<String, String> attributes = job.getJobDescriptor().getAttributes();
        if (attributes.containsKey(MOCK_VK_PROPERTY_PREPARE_TIME) || attributes.containsKey(MOCK_VK_PROPERTY_RUN_TIME) || attributes.containsKey(MOCK_VK_PROPERTY_KILL_TIME)) {
            Map<String, String> annotations = new HashMap<>();
            Evaluators.acceptNotNull(attributes.get(MOCK_VK_PROPERTY_PREPARE_TIME), value -> annotations.put(PREPARE_TIME, value));
            Evaluators.acceptNotNull(attributes.get(MOCK_VK_PROPERTY_RUN_TIME), value -> annotations.put(RUN_TIME, value));
            Evaluators.acceptNotNull(attributes.get(MOCK_VK_PROPERTY_KILL_TIME), value -> annotations.put(KILL_TIME, value));
            return annotations;
        }
        // Legacy
        return findLegacyTaskLifecycleEnv(job.getJobDescriptor().getContainer().getEnv())
                .map(PerformanceToolUtil::toLegacyAnnotations)
                .orElse(Collections.emptyMap());
    }

    private static Optional<String> findLegacyTaskLifecycleEnv(Map<String, String> env) {
        return env.keySet().stream().filter(k -> k.startsWith("TASK_LIFECYCLE")).map(env::get).findFirst();
    }

    private static Map<String, String> toLegacyAnnotations(String envValue) {
        Map<String, String> annotations = new HashMap<>();

        Matcher matcher = TASK_STATE_RULES_RE.matcher(envValue);
        while (matcher.find()) {
            String state = matcher.group(1);
            String delayWithUnits = matcher.group(2);
            long delayMs = TimeUnitExt.toMillis(delayWithUnits).orElse(-1L);
            if (delayMs > 0) {
                switch (state) {
                    case "startInitiated":
                        annotations.put(PREPARE_TIME, delayWithUnits);
                        break;
                    case "started":
                        annotations.put(RUN_TIME, delayWithUnits);
                        break;
                    case "killInitiated":
                        annotations.put(KILL_TIME, delayWithUnits);
                        break;
                }
            }
        }

        return annotations;
    }
}
