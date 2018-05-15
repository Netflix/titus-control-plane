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

package com.netflix.titus.api.jobmanager;

/**
 * Constant keys for Job attributes.
 */
public final class JobParameterAttributes {
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING = "titusParameter.agent.allowCpuBursting";
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING = "titusParameter.agent.allowNetworkBursting";
    public static final String JOB_PARAMETER_ATTRIBUTES_BATCH = "titusParameter.agent.batch";
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_NESTED_CONTAINERS = "titusParameter.agent.allowNestedContainers";
    public static final String JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS = "titusParameter.agent.killWaitSeconds";

    private JobParameterAttributes() {
    }
}
