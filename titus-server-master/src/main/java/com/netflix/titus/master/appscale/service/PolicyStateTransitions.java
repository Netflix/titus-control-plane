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
package com.netflix.titus.master.appscale.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.appscale.model.PolicyStatus;

public class PolicyStateTransitions {
    private static Map<PolicyStatus, List<PolicyStatus>> allowedTransitions = new HashMap<PolicyStatus, List<PolicyStatus>>() {{
        put(PolicyStatus.Applied, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Pending);
            add(PolicyStatus.Deleting);
        }});

        put(PolicyStatus.Pending, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Pending);
            add(PolicyStatus.Applied);
            add(PolicyStatus.Error);
        }});

        put(PolicyStatus.Deleting, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Deleted);
        }});
        put(PolicyStatus.Deleted, Collections.emptyList());
        put(PolicyStatus.Error, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Deleting);
            add(PolicyStatus.Pending);
        }});
    }};

    public static boolean isAllowed(PolicyStatus from, PolicyStatus to) {
        return allowedTransitions.containsKey(from) && allowedTransitions.get(from).contains(to);
    }
}
