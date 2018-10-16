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

public interface AgentQualityTracker {

    /**
     * Evaluates quality of an agent with the given id from 0 to 1 (0 == do not use it, 1 == perfect).
     *
     * @param agentHostName an agent host name
     * @return the agent's quality score from 0 to 1, or -1 if there is no agent with the given id
     */
    double qualityOf(String agentHostName);
}
