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

package com.netflix.titus.api.jobmanager;

public final class TaskAttributes {
    /*
     * Agent attributes.
     */
    public static final String TASK_ATTRIBUTES_AGENT_ID = "agent.id";
    public static final String TASK_ATTRIBUTES_AGENT_REGION = "agent.region";
    public static final String TASK_ATTRIBUTES_AGENT_ZONE = "agent.zone";
    public static final String TASK_ATTRIBUTES_AGENT_ASG = "agent.asg";
    public static final String TASK_ATTRIBUTES_AGENT_HOST = "agent.host";
    public static final String TASK_ATTRIBUTES_AGENT_INSTANCE_ID = "agent.instanceId";
    public static final String TASK_ATTRIBUTES_AGENT_ITYPE = "agent.itype";

    /*
     * Task attributes.
     */
    public static final String TASK_ATTRIBUTES_TASK_INDEX = "task.index";
    public static final String TASK_ATTRIBUTES_TASK_RESUBMIT_OF = "task.resubmitOf";
    public static final String TASK_ATTRIBUTES_TASK_ORIGINAL_ID = "task.originalId";
    public static final String TASK_ATTRIBUTES_RESUBMIT_NUMBER = "task.resubmitNumber";
    public static final String TASK_ATTRIBUTES_SYSTEM_RESUBMIT_NUMBER = "task.systemResubmitNumber";
    public static final String TASK_ATTRIBUTES_EVICTION_RESUBMIT_NUMBER = "task.evictionResubmitNumber";
    public static final String TASK_ATTRIBUTES_RETRY_DELAY = "task.retryDelay";
    public static final String TASK_ATTRIBUTES_CONTAINER_IP = "task.containerIp";
    public static final String TASK_ATTRIBUTES_NETWORK_INTERFACE_ID = "task.networkInterfaceId";
    public static final String TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX = "task.networkInterfaceIndex";
    public static final String TASK_ATTRIBUTES_EXECUTOR_URI_OVERRIDE = "task.executorUriOverride";
    public static final String TASK_ATTRIBUTES_TIER = "task.tier";
    public static final String TASK_ATTRIBUTES_IP_ALLOCATION_ID = "task.ipAllocationId";
    public static final String TASK_ATTRIBUTES_IN_USE_IP_ALLOCATION = "task.ipAllocationAlreadyInUseByTask";

    /**
     * Task moved from one job to another.
     */
    public static final String TASK_ATTRIBUTES_MOVED_FROM_JOB = "task.movedFromJob";

    /*
     * Cell info.
     */
    public static final String TASK_ATTRIBUTES_CELL = JobAttributes.JOB_ATTRIBUTES_CELL;
    public static final String TASK_ATTRIBUTES_STACK = JobAttributes.JOB_ATTRIBUTES_STACK;

    private TaskAttributes() {
    }
}
