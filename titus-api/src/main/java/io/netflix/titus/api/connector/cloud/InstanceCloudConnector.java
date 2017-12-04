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

package io.netflix.titus.api.connector.cloud;

import java.util.List;
import java.util.Optional;

import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.util.tuple.Either;
import rx.Completable;
import rx.Observable;

/**
 * The primary API to interact with cloud providers.
 */
public interface InstanceCloudConnector {

    /**
     * Get all Titus instance groups.
     */
    Observable<List<InstanceGroup>> getInstanceGroups();

    /**
     * Get instance groups by id. The result contains all instance groups that exist. Non-existent instance groups
     * are silently ignored.
     */
    Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds);

    /**
     * Get instance group launch configurations.
     */
    Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds);

    /**
     * Returns resource dimension (cpu, memory, etc) for the given instance type.
     *
     * @throws CloudConnectorException with 'NotFound' status code if instance type is not recognized
     */
    ResourceDimension getInstanceTypeResourceDimension(String instanceType);

    /**
     * Get detailed information about instances with the given ids.
     */
    Observable<List<Instance>> getInstances(List<String> instanceIds);

    /**
     * Get detailed information about instances with the given instance group id.
     */
    Observable<List<Instance>> getInstancesByInstanceGroupId(String instanceGroupId);

    /**
     * Change instance group capacity.
     */
    Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired);

    /**
     * Increase instance group size by the given number of instances. The following constraints are checked prior
     * to admitting scale up:
     * <ul>
     * <li>scaleUpCount >= 0 (if scaleUpCount == 0, the operation is void)</li>
     * <li>desired + scaleUpCount <= max instance group size (throws an error if not)</li>
     * </ul>
     *
     * @param scaleUpCount number of instances to add (must be >= 0)
     */
    Completable scaleUp(String instanceGroupId, int scaleUpCount);

    /**
     * Terminate multiple cloud instances. For each successfully terminated instance return {@link Optional#empty()} or
     * an optional with the exception value.
     * <p>
     * TODO Better handle termination of a set where some instances no longer exist
     */
    Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink);
}
