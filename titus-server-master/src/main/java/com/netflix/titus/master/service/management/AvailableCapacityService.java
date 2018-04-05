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

package com.netflix.titus.master.service.management;

import java.util.Optional;

import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;

/**
 * Provides information about total available capacity within all tiers.
 * <p>
 * TODO This interface is very close to {@link CapacityAllocationService}, and probably we should merge them into single entity
 */
public interface AvailableCapacityService {

    Optional<ResourceDimension> totalCapacityOf(Tier tier);
}
