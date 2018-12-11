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

package com.netflix.titus.supplementary.relocation;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus.TaskRelocationState;

public class TaskRelocationPlanGenerator {

    public static TaskRelocationPlan oneMigrationPlan() {
        return TaskRelocationPlan.newBuilder()
                .withTaskId("task1")
                .withRelocationTime(123)
                .withReason(TaskRelocationReason.TaskMigration)
                .withReasonMessage("test")
                .build();
    }

    public static TaskRelocationStatus oneSuccessfulRelocation() {
        return TaskRelocationStatus.newBuilder()
                .withTaskId("task1")
                .withState(TaskRelocationState.Success)
                .withStatusCode(TaskRelocationStatus.STATUS_CODE_TERMINATED)
                .withStatusMessage("Successfully terminated")
                .withTaskRelocationPlan(oneMigrationPlan())
                .build();
    }
}
