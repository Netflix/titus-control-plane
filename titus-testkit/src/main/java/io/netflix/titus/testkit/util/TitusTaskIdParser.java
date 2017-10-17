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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TitusTaskIdParser {

    private static final Pattern TASK_PATTERN = Pattern.compile("(Titus-\\d+)-worker-(\\d+)-(\\d+)");

    private TitusTaskIdParser() {
    }

    public static String getJobIdFromTaskId(String taskId) {
        return getMatcher(taskId).group(1);
    }

    public static int getTaskIndexFromTaskId(String taskId) {
        return Integer.parseInt(getMatcher(taskId).group(2));
    }

    public static int getTaskNumberFromTaskId(String taskId) {
        return Integer.parseInt(getMatcher(taskId).group(3));
    }

    private static Matcher getMatcher(String taskId) {
        Matcher matcher = TASK_PATTERN.matcher(taskId);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Task id does not conform to the expected format " + TASK_PATTERN.toString());
        }
        return matcher;
    }
}
