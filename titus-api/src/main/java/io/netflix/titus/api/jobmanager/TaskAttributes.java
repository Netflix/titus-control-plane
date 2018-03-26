/*
 * Copyright 2017 Netflix, Inc.
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

public final class TaskAttributes {
    /*
     * Agent attributes.
     */
    public static final String TASK_ATTRIBUTES_AGENT_REGION = "agent.region";
    public static final String TASK_ATTRIBUTES_AGENT_ZONE = "agent.zone";
    public static final String TASK_ATTRIBUTES_AGENT_HOST = "agent.host";
    public static final String TASK_ATTRIBUTES_AGENT_INSTANCE_ID = "agent.instanceId";

    /*
     * Task attributes.
     */
    public static final String TASK_ATTRIBUTES_TASK_INDEX = "task.index";
    public static final String TASK_ATTRIBUTES_TASK_RESUBMIT_OF = "task.resubmitOf";
    public static final String TASK_ATTRIBUTES_TASK_ORIGINAL_ID = "task.originalId";
    public static final String TASK_ATTRIBUTES_V2_TASK_ID = "v2.taskId";
    public static final String TASK_ATTRIBUTES_V2_TASK_INSTANCE_ID = "v2.taskInstanceId";
    public static final String TASK_ATTRIBUTES_RESUBMIT_NUMBER = "task.resubmitNumber";
    public static final String TASK_ATTRIBUTES_SYSTEM_RESUBMIT_NUMBER = "task.systemResubmitNumber";
    public static final String TASK_ATTRIBUTES_RETRY_DELAY = "task.retryDelay";
    public static final String TASK_ATTRIBUTES_CONTAINER_IP = "task.containerIp";
    public static final String TASK_ATTRIBUTES_NETWORK_INTERFACE_ID = "task.networkInterfaceId";
    public static final String TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX = "task.networkInterfaceIndex";
    public static final String TASK_ATTRIBUTES_EXECUTOR_URI_OVERRIDE = "task.executorUriOverride";

    /*
     * Cell info.
     */
    public static final String TASK_ATTRIBUTES_CELL = JobAttributes.JOB_ATTRIBUTES_CELL;
    public static final String TASK_ATTRIBUTES_STACK = JobAttributes.JOB_ATTRIBUTES_STACK;

    private TaskAttributes() {
    }
}
