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

package io.netflix.titus.api.jobmanager;

/**
 * Constant keys for Job attributes.
 */
public final class JobAttributes {
    public static final String JOB_ATTRIBUTES_ALLOW_CPU_BURSTING = "titus.agent.allowCpuBursting";
    public static final String JOB_ATTRIBUTES_ALLOW_NETWORK_BURSTING = "titus.agent.allowNetworkBursting";
    public static final String JOB_ATTRIBUTES_BATCH = "titus.agent.batch";
    public static final String JOB_ATTRIBUTES_ALLOW_NESTED_CONTAINERS = "titus.agent.allowNestedContainers";
    public static final String JOB_ATTRIBUTES_KILL_WAIT_SECONDS = "titus.agent.killWaitSeconds";
    /**
     * Stack name that can be replaced in a federated deployment, where all Cells have the same Stack name.
     */
    public static final String JOB_ATTRIBUTES_STACK = "titus.stack";
    /**
     * Unique Cell name for a deployment.
     */
    public static final String JOB_ATTRIBUTES_CELL = "titus.cell";

    private JobAttributes() {
    }
}
