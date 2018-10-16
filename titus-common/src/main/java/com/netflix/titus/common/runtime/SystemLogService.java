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

package com.netflix.titus.common.runtime;

/**
 * An API for processing system events (errors, exceptions, invariant violations, important configuration changes, etc)
 * with supplementary metadata information. Those events are sent to external system(s) for further processing
 * and automated analysis and reporting.
 */
public interface SystemLogService {

    /**
     * Write an event to an external system for further processing.
     *
     * @return true if the event was accepted, false otherwise. Accepting an event does not mean that it was or will be
     * successfully delivered to an external event store. False result means that the event consumer is overloaded.
     * {@link SystemLogService} implementation may choose in the latter case to accept higher priority events over
     * the lower priority ones.
     */
    boolean submit(SystemLogEvent event);
}
