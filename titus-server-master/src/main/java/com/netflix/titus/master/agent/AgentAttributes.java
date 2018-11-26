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

package com.netflix.titus.master.agent;

public final class AgentAttributes {

    /**
     * Mark an agent such that it will not get terminated by the cluster operations component.
     */
    public static final String NOT_REMOVABLE = "titus.notRemovable";

    /**
     * Mark an agent such that it will get terminated. Note that the cluster operations component
     * will mark an agent once it determines it can terminate the agent
     */
    public static final String REMOVABLE = "titus.removable";

    /**
     * Mark an agent such that no placements will happen.
     */
    public static final String NO_PLACEMENT = "titus.noPlacement";

    /**
     * Mark an agent such that placements will not be preferred on this agent.
     */
    public static final String PREFER_NO_PLACEMENT = "titus.preferNoPlacement";

    /**
     * Mark an agent such that all containers will be evicted based on the job disruption budget.
     */
    public static final String EVICTABLE = "titus.evictable";

    /**
     * Mark an agent such that all containers will be immediately evicted.
     */
    public static final String EVICTABLE_IMMEDIATELY = "titus.evictableImmediately";

}
