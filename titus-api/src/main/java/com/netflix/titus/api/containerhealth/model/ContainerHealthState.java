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

package com.netflix.titus.api.containerhealth.model;

public enum ContainerHealthState {
    /**
     * The application process running in the container is healthy, and ready to handle user requests.
     */
    Healthy,

    /**
     * The application process running in the container is not running or in broken state.
     * It is not ready to handle user requests. In the job disruption budget, it should be counted as not available.
     */
    Unhealthy,

    /**
     * The health status of the application running in the container is not known.
     */
    Unknown,

    /**
     * The application/container are terminated. All tasks which are finished are assigned this state.
     */
    Terminated
}
