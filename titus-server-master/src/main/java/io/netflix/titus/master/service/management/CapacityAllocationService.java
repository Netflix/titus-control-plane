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

package io.netflix.titus.master.service.management;

import java.util.List;

import rx.Observable;

/**
 * {@link CapacityAllocationService} manages agent cluster size limits.
 */
public interface CapacityAllocationService {

    /**
     * For each instance type, returns its maximum server group size.
     */
    Observable<List<Integer>> limits(List<String> instanceTypes);

    /**
     * Ensure minimum capacity for the given instance type.
     *
     * @param instanceType  AWS instance type
     * @param instanceCount minimum number of live instances always expected to run
     * @return observable that must be subscribed to, to trigger the scale up/down action. It is completed once
     * the scale up/down action has been successfully submitted.
     */
    Observable<Void> allocate(String instanceType, int instanceCount);
}
