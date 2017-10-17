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

package io.netflix.titus.master.endpoint.v2.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.model.v2.V2JobState;

public final class QueryParametersUtil {

    private static final List<TitusTaskState> ALL_TITUS_TASK_STATES = Collections.unmodifiableList(new ArrayList<>(TitusTaskState.ANY));

    private static final Pattern LABEL_VALUE_RE = Pattern.compile("([^=,]*)(=([^,]*)?)?,?");

    private QueryParametersUtil() {
    }

    public static List<TitusTaskState> getTaskStatesFromParams(Collection<String> taskStatesParams) {
        if (taskStatesParams == null || taskStatesParams.isEmpty()) {
            return Collections.emptyList();
        }
        return getTaskStatesFromParams(taskStatesParams.toArray(new String[taskStatesParams.size()]));
    }

    public static List<TitusTaskState> getTaskStatesFromParams(String[] taskStatesParams) {
        if (taskStatesParams == null || taskStatesParams.length == 0) {
            return Collections.emptyList();
        }
        List<TitusTaskState> taskStates = new ArrayList<>();
        for (String s : taskStatesParams) {
            if ("ANY".equalsIgnoreCase(s)) {
                return ALL_TITUS_TASK_STATES;
            }
            final TitusTaskState state = TitusTaskState.toTitusState(s);
            if (state != null) {
                taskStates.add(state);
            }
        }
        return taskStates;
    }

    public static Map<String, String> buildLabelMap(List<String> queryLabels) {
        if (queryLabels == null || queryLabels.isEmpty()) {
            return Collections.emptyMap();
        }
        return buildLabelMap(queryLabels.toArray(new String[queryLabels.size()]));
    }

    public static Map<String, String> buildLabelMap(String[] queryLabels) {
        Map<String, String> result = new HashMap<>();
        for (String labelValueStr : queryLabels) {
            buildLabelMapAndAppend(labelValueStr, result);
        }
        return result;
    }

    public static boolean hasArchivedState(Collection<TitusTaskState> taskStates) {
        if (taskStates != null && !taskStates.isEmpty()) {
            for (TitusTaskState s : taskStates) {
                if (TitusTaskState.getV2State(s) == V2JobState.Failed) {
                    return true;
                }
            }
        }
        return false;
    }

    /* Visible for testing */
    static void buildLabelMapAndAppend(String labelValueStr, Map<String, String> result) {
        Matcher matcher = LABEL_VALUE_RE.matcher(labelValueStr);
        do {
            matcher.find(); // always matches
            String label = trimLabel(matcher.group(1));
            if (label != null) {
                result.put(label, trimLabel(matcher.group(3)));
            }
        } while ((matcher.end() < labelValueStr.length()));
    }

    /**
     * Trim the given label value. If it is empty string, return null.
     */
    static String trimLabel(String labelValue) {
        if (labelValue == null) {
            return null;
        }
        String trimmed = labelValue.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
