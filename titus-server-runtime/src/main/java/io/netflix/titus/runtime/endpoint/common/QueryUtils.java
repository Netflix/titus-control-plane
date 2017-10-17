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

package io.netflix.titus.runtime.endpoint.common;

import java.util.Map;
import java.util.Set;

public final class QueryUtils {

    public static boolean matchesAttributes(Map<String, Set<String>> expectedLabels, Map<String, String> labels, boolean andOp) {
        if (labels.isEmpty()) {
            return false;
        }
        // and
        if (andOp) {
            for (Map.Entry<String, Set<String>> expected : expectedLabels.entrySet()) {
                String expectedKey = expected.getKey();
                if (!labels.containsKey(expectedKey)) {
                    return false;
                }
                Set<String> expectedValues = expected.getValue();
                if (!expectedValues.isEmpty() && !expectedValues.contains(labels.get(expectedKey))) {
                    return false;
                }
            }
            return true;
        }

        // or
        for (Map.Entry<String, Set<String>> expected : expectedLabels.entrySet()) {
            String expectedKey = expected.getKey();
            if (labels.containsKey(expectedKey)) {
                Set<String> expectedValues = expected.getValue();
                if (expectedValues.isEmpty() || expectedValues.contains(labels.get(expectedKey))) {
                    return true;
                }
            }
        }
        return false;
    }
}
