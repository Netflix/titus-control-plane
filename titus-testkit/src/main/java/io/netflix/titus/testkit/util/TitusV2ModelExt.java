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

package io.netflix.titus.testkit.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.common.util.CollectionsExt;

/**
 * Collection of helper functions to deal with Titus v2 data model entity.
 */
public final class TitusV2ModelExt {

    private TitusV2ModelExt() {
    }

    public static Map<String, TaskInfo> toMap(Collection<TaskInfo> tasks) {
        if (CollectionsExt.isNullOrEmpty(tasks)) {
            return Collections.emptyMap();
        }
        Map<String, TaskInfo> result = new HashMap<>();
        tasks.forEach(t -> result.put(t.getId(), t));
        return result;
    }
}
