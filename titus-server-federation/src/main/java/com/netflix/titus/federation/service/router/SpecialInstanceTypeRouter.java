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

package com.netflix.titus.federation.service.router;

import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.JobDescriptor;

/**
 * {@link CellRouter} for special instance types, like GPU instances. With this selector the specialized and expensive
 * instance types can be centralized in one cell, and all jobs that require these resources can be routed to it
 * irrespective of to which application they belong to.
 * <br/>
 * If a job requires specialized resources, a cell which provides these resources is returned. If a job does not
 * require specialized resources or none of the cells provide it, the result is {@link Optional#empty()}.
 */
public class SpecialInstanceTypeRouter implements CellRouter {

    private static final String REGULAR = "regularInstanceType";
    private static final String SPECIAL_INSTANCE_GPU = "gpu";

    private final Function<JobDescriptor, String> instanceTypeRouteKeyResolver;
    private final RoutingRuleSelector selector;

    public SpecialInstanceTypeRouter(CellInfoResolver cellInfoResolver,
                                     Function<JobDescriptor, String> instanceTypeRouteKeyResolver,
                                     TitusFederationConfiguration federationConfiguration) {
        this.instanceTypeRouteKeyResolver = instanceTypeRouteKeyResolver;
        this.selector = new RoutingRuleSelector(cellInfoResolver, federationConfiguration::getInstanceTypeRoutingRules);

    }

    @Override
    public Optional<Cell> routeKey(JobDescriptor jobDescriptor) {
        return selector.select(instanceTypeRouteKeyResolver.apply(jobDescriptor), c -> true);
    }

    public static SpecialInstanceTypeRouter getGpuInstanceTypeRouter(CellInfoResolver cellInfoResolver,
                                                                     TitusFederationConfiguration federationConfiguration) {
        return new SpecialInstanceTypeRouter(
                cellInfoResolver,
                jobDescriptor -> jobDescriptor.getContainer().getResources().getGpu() > 0 ? SPECIAL_INSTANCE_GPU : REGULAR,
                federationConfiguration
        );
    }
}
