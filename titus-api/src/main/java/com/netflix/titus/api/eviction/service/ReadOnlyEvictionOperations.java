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

package com.netflix.titus.api.eviction.service;

import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.model.Tier;
import rx.Observable;

public interface ReadOnlyEvictionOperations {

    EvictionQuota getGlobalEvictionQuota();

    EvictionQuota getTierEvictionQuota(Tier tier);

    EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName);

    Optional<EvictionQuota> findJobEvictionQuota(String jobId);

    Observable<EvictionEvent> events(boolean includeSnapshot);
}
