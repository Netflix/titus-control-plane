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

package com.netflix.titus.api.agent.model;

public enum InstanceGroupLifecycleState {
    /**
     * Server group is not accepting any traffic or auto-scale actions.
     */
    Inactive,

    /**
     * Server group is open for traffic, and auto-scaling.
     */
    Active,

    /**
     * Server group is open for traffic, and auto-scaling, but other server groups are preferred.
     */
    PhasedOut,

    /**
     * Server group is not accepting any traffic. All idle instances will be terminated.
     */
    Removable
}
