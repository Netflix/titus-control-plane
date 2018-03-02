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

package io.netflix.titus.api.scheduler.model.sanitizer;

import java.util.HashMap;
import java.util.Map;

import io.netflix.titus.api.scheduler.model.SystemSelector;

public class SchedulerAssertions {

    public Map<String, String> validateSystemSelector(SystemSelector systemSelector) {
        Map<String, String> violations = new HashMap<>();

        boolean validShould = systemSelector.getShould() != null;
        boolean validMust = systemSelector.getMust() != null;

        if (!validShould && !validMust) {
            violations.put("", "must specify a valid should or must");
        }

        if (validShould && validMust) {
            violations.put("", "must specify only a valid should or must and not both");
        }

        return violations;
    }
}
