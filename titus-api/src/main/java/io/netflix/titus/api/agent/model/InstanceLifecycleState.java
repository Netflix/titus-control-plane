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

package io.netflix.titus.api.agent.model;

public enum InstanceLifecycleState {

    /**
     * Initial state of an agent, set when first discovered.
     */
    Launching,

    /**
     * An agent instance can move to this state only from the 'Launched' state. This happens as soon as all
     * healthcheck indicators for the agent are ok. At this point, the agent can accept containers, provided
     * that other criteria are fullfiled.
     */
    Started,

    /**
     * An agent instance is terminating. No new work is accepted.
     */
    KillInitiated,

    /**
     * An agent instance is not running anymore.
     */
    Stopped,

    /**
     * An agent state is unknown at this point in time.
     */
    Unknown
}
