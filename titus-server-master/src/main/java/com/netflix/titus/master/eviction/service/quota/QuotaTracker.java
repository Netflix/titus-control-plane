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

package com.netflix.titus.master.eviction.service.quota;

/**
 * {@link QuotaTracker} provides information about the available quota. If the quota counter is managed explicitly,
 * the {@link QuotaController} provides additional method to consume the quota. For other cases, quota is computed from
 * other information, and cannot be changed directly. For example quota for tasks that can be terminated is a function
 * of tasks being in healthy and unhealthy state. Terminating a task means, the quota will be decreased by one.
 */
public interface QuotaTracker {

    long getQuota();
}
