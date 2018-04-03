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

package com.netflix.titus.master.scheduler;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.netflix.fenzo.AssignmentFailure;
import com.netflix.fenzo.ConstraintFailure;
import com.netflix.fenzo.TaskAssignmentResult;

public class SimpleFailuresAnalyzer {
    private final String taskId;
    private final List<TaskAssignmentResult> failures;

    public SimpleFailuresAnalyzer(String taskId, List<TaskAssignmentResult> failures) {
        this.taskId = taskId;
        this.failures = failures;
    }

    public Map<String, Object> getAnalysisAsMapOfStrings() {
        if (failures == null || failures.isEmpty()) {
            return Collections.emptyMap();
        }

        // This is a Map of <Constraint, Map<ConstraintFailReason, List<Hostnames>>>
        Map<String, Map<String, List<String>>> constraintFailures = new HashMap<>();
        // This is a Map if <Resource, List<Hostname(shortage)>>
        Map<String, List<String>> assignmntFailures = new HashMap<>();
        for (TaskAssignmentResult f : failures) {
            // build constraint failures
            final ConstraintFailure c = f.getConstraintFailure();
            if (c != null) {
                final String name = c.getName();
                constraintFailures.putIfAbsent(name, new HashMap<>());
                constraintFailures.get(name).putIfAbsent(c.getReason(), new LinkedList<>());
                constraintFailures.get(name).get(c.getReason()).add(f.getHostname());
            }

            // build assignment failures
            final List<AssignmentFailure> failures = f.getFailures();
            if (failures != null && !failures.isEmpty()) {
                for (AssignmentFailure af : failures) {
                    assignmntFailures.putIfAbsent(af.getResource().toString(), new LinkedList<>());
                    assignmntFailures.get(af.getResource().toString()).add(f.getHostname() + "(" +
                            (af.getAvailable() - af.getUsed() - af.getAsking()) + "): " + af.getMessage());
                }
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put("ConstraintFailures", constraintFailures);
        result.put("ResourceFailures", assignmntFailures);
        return result;
    }
}
