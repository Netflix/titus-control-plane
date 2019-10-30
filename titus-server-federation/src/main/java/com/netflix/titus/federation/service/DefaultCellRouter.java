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

package com.netflix.titus.federation.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultCellRouter implements CellRouter {
    private static final Logger logger = LoggerFactory.getLogger(DefaultCellRouter.class);
    // The key used to route jobs if no other key could be determined.
    private static final String DEFAULT_ROUTE_KEY = "DEFAULT_FEDERATION_ROUTE_KEY";

    private final CellInfoResolver cellInfoResolver;
    private final TitusFederationConfiguration federationConfiguration;
    private final Function<String, Map<Cell, Pattern>> compileRoutingPatterns;

    @Inject
    public DefaultCellRouter(CellInfoResolver cellInfoResolver, TitusFederationConfiguration federationConfiguration) {
        this.cellInfoResolver = cellInfoResolver;
        this.federationConfiguration = federationConfiguration;

        compileRoutingPatterns = Evaluators.memoizeLast((spec, lastCompiledPatterns) -> {
            logger.info("Detected new routing rules, compiling them: {}", spec);
            List<Cell> cells = cellInfoResolver.resolve();
            Map<Cell, String> cellRoutingRules = CellInfoUtil.extractCellRoutingFromCellSpecification(cells, spec);
            try {
                return CollectionsExt.mapValues(cellRoutingRules, Pattern::compile, LinkedHashMap::new);
            } catch (PatternSyntaxException e) {
                logger.error("Bad cell routing spec, ignoring: {}", spec);
                return lastCompiledPatterns.orElseThrow(() -> e /* there is nothing to do if the first spec is bad */);
            }
        });

        // ensure the initial spec can be compiled or fail fast
        compileRoutingPatterns.apply(federationConfiguration.getRoutingRules());
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
    public Cell routeKey(JobDescriptor jobDescriptor) {
        Optional<Cell> pinnedCell = getCellPinnedToJob(jobDescriptor);
        if (pinnedCell.isPresent()) {
            return pinnedCell.get();
        }

        String routeKey = routeKeyFor(jobDescriptor);
        Map<Cell, Pattern> cellRoutingPatterns = compileRoutingPatterns.apply(federationConfiguration.getRoutingRules());
        Set<String> antiAffinityNames = StringExt.splitByCommaIntoSet(
                jobDescriptor.getAttributesMap().get(JobAttributes.JOB_PARAMETER_ATTRIBUTES_CELL_AVOID)
        );


        Optional<Cell> found = cellRoutingPatterns.entrySet().stream()
                .filter(entry -> entry.getValue().matcher(routeKey).matches() &&
                        !antiAffinityNames.contains(entry.getKey().getName()))
                .findFirst()
                .map(Map.Entry::getKey);

        if (found.isPresent()) {
            return found.get();
        }

        // fallback to any cell that is not in the anti affinity list, or the default when all available cells are rejected
        return cellInfoResolver.resolve().stream()
                .filter(cell -> !antiAffinityNames.contains(cell.getName()))
                .findAny()
                .orElseGet(cellInfoResolver::getDefault);
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
