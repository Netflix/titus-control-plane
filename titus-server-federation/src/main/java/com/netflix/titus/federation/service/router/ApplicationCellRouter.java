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
import java.util.Set;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.JobDescriptor;

/**
 * Route using the application name or a capacity group.
 */
public class ApplicationCellRouter implements CellRouter {

    // The key used to route jobs if no other key could be determined.
    private static final String DEFAULT_ROUTE_KEY = "DEFAULT_FEDERATION_ROUTE_KEY";

    private final CellInfoResolver cellInfoResolver;
    private final RoutingRuleSelector selector;

    public ApplicationCellRouter(CellInfoResolver cellInfoResolver, TitusFederationConfiguration federationConfiguration) {
        this.cellInfoResolver = cellInfoResolver;
        this.selector = new RoutingRuleSelector(cellInfoResolver, federationConfiguration::getRoutingRules);
    }

    /**
     * routeKeyFor is a memory-based, blocking call that extracts a Cell route key from a JobDescriptor.
     *
     * @param jobDescriptor
     * @return key used for finding a suitable cell from configured routing rules
     */
    private static String routeKeyFor(JobDescriptor jobDescriptor) {
        if (!jobDescriptor.getCapacityGroup().isEmpty()) {
            return jobDescriptor.getCapacityGroup();
        }
        if (!jobDescriptor.getApplicationName().isEmpty()) {
            return jobDescriptor.getApplicationName();
        }
        return DEFAULT_ROUTE_KEY;
    }

    /**
     * Iterate each of the regular expressions in the order they are defined, and returns the first match it encounters.
     * If no match, defaults to {@link CellInfoResolver#getDefault()}.
     *
     * @param jobDescriptor
     */
    @Override
    public Optional<Cell> routeKey(JobDescriptor jobDescriptor) {
        Optional<Cell> pinnedCell = getCellPinnedToJob(jobDescriptor);
        if (pinnedCell.isPresent()) {
            return pinnedCell;
        }

        String routeKey = routeKeyFor(jobDescriptor);
        Set<String> antiAffinityNames = StringExt.splitByCommaIntoSet(
                jobDescriptor.getAttributesMap().get(JobAttributes.JOB_PARAMETER_ATTRIBUTES_CELL_AVOID)
        );

        Optional<Cell> found = selector.select(routeKey, cell -> !antiAffinityNames.contains(cell.getName()));
        if (found.isPresent()) {
            return found;
        }

        // fallback to any cell that is not in the anti affinity list, or the default when all available cells are rejected
        return cellInfoResolver.resolve().stream()
                .filter(cell -> !antiAffinityNames.contains(cell.getName()))
                .findAny();
    }

    private Optional<Cell> getCellPinnedToJob(JobDescriptor jobDescriptor) {
        String requestedCell = jobDescriptor.getAttributesMap().get(JobAttributes.JOB_PARAMETER_ATTRIBUTES_CELL_REQUEST);
        if (StringExt.isEmpty(requestedCell)) {
            return Optional.empty();
        }
        return cellInfoResolver.resolve().stream()
                .filter(cell -> cell.getName().equals(requestedCell))
                .findAny();
    }
}
