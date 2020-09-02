/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime;

/**
 * Collection of task relocation related attributes that can be associated with agent instance groups, agent instances,
 * jobs or tasks. These attributes when set, affect the relocation process of the tagged object.
 */
public final class RelocationAttributes {

    /**
     * If set to true, marks an object as needing migration. Applicable to an agent instance and a task.
     */
    public static final String RELOCATION_REQUIRED = "titus.relocation.required";

    /**
     * Like {@link #RELOCATION_REQUIRED}, but the safeguards (system eviction limits, job disruption budget)
     * for the object termination are ignored.
     */
    public static final String RELOCATION_REQUIRED_IMMEDIATELY = "titus.relocation.requiredImmediately";

    /**
     * If set to true, marks an object as needing immediate migration if its creation time happened before the time set as
     * an attribute value. The timestamp value is an epoch.
     * Settable on a job, and applied to all tasks belonging to the given job.
     */
    public static final String RELOCATION_REQUIRED_BY = "titus.relocation.requiredBy";

    /**
     * Like {@link #RELOCATION_REQUIRED_BY}, but the safeguards (system eviction limits, job disruption budget)
     * for the object termination are ignored.
     */
    public static final String RELOCATION_REQUIRED_BY_IMMEDIATELY = "titus.relocation.requiredByImmediately";

    /**
     * If set to true, turns off the relocation process for an object. Applicable to an agent, a job and a task.
     */
    public static final String RELOCATION_NOT_ALLOWED = "titus.relocation.notAllowed";
}
