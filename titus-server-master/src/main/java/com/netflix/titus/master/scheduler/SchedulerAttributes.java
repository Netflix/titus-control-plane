/*
 * Copyright 2019 Netflix, Inc.
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

/**
 * Collection of scheduler related attributes that can be associated with agent instance groups and agent instances.
 */
public final class SchedulerAttributes {

    /**
     * Mark an instance group or agent instance such that placements will not happen on this instance group or agent instance. This will
     * be used by the system in order to restrict placements without affecting the {@link #NO_PLACEMENT} attribute.
     */
    public static final String SYSTEM_NO_PLACEMENT = "titus.scheduler.systemNoPlacement";

    /**
     * Mark an instance group or agent instance such that placements will not happen on this instance group or agent instance.
     */
    public static final String NO_PLACEMENT = "titus.scheduler.noPlacement";

    /**
     * Mark an agent such that placements will not be preferred on this instance group.
     */
    public static final String PREFER_NO_PLACEMENT = "titus.scheduler.preferNoPlacement";

    /**
     * Mark an agent instance or instance group such that placements will only place if the job has a toleration that matches.
     */
    public static final String TAINTS = "titus.scheduler.taints";
}
