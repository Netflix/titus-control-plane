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

package com.netflix.titus.master.kubernetes.controller;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.kubernetes.controller")
public interface ControllerConfiguration {

    /**
     * @return whether or not the controller is enabled
     */
    @DefaultValue("true")
    boolean isControllerEnabled();

    /**
     * @return the initial delay in milliseconds before the execution runs after process startup.
     */
    @DefaultValue("10000")
    long getControllerInitialDelayMs();

    /**
     * @return the interval in milliseconds of how often the controller runs.
     */
    @DefaultValue("30000")
    long getControllerIntervalMs();

    /**
     * @return the timeout of the controller's execution loop.
     */
    @DefaultValue("60000")
    long getControllerTimeoutMs();
}
