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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.List;
import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;

public interface DeschedulerService {

    List<DeschedulingResult> deschedule(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans);
}
