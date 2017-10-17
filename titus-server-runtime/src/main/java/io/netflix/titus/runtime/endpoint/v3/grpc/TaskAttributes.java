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

package io.netflix.titus.runtime.endpoint.v3.grpc;

public class TaskAttributes {
    /*
     * Agent attributes.
     */
    public static String TASK_ATTRIBUTES_AGENT_REGION = "agent.region";
    public static String TASK_ATTRIBUTES_AGENT_ZONE = "agent.zone";
    public static String TASK_ATTRIBUTES_AGENT_HOST = "agent.host";
    public static String TASK_ATTRIBUTES_AGENT_INSTANCE_ID = "agent.instanceId";

    /*
     * Task attributes.
     */
    public static String TASK_ATTRIBUTES_TASK_INDEX = "task.index";
    public static String TASK_ATTRIBUTES_TASK_RESUBMIT_OF = "task.resubmitOf";
    public static String TASK_ATTRIBUTES_TASK_ORIGINAL_ID = "task.originalId";
    public static String TASK_ATTRIBUTES_V2_TASK_ID = "v2.taskId";
    public static String TASK_ATTRIBUTES_V2_TASK_INSTANCE_ID = "v2.taskInstanceId";
    public static String TASK_ATTRIBUTES_RESUBMIT_NUMBER = "task.resubmitNumber";
    public static String TASK_ATTRIBUTES_CONTAINER_IP = "task.containerIp";
}
