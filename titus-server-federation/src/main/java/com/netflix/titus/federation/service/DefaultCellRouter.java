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
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.cache.MemoizedFunction;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultCellRouter implements CellRouter {
    private static final Logger logger = LoggerFactory.getLogger(DefaultCellRouter.class);

    private final CellInfoResolver cellInfoResolver;
    private final TitusFederationConfiguration federationConfiguration;
    private final Function<String, Map<Cell, Pattern>> compileRoutingPatterns;

    @Inject
    public DefaultCellRouter(CellInfoResolver cellInfoResolver, TitusFederationConfiguration federationConfiguration) {
        this.cellInfoResolver = cellInfoResolver;
        this.federationConfiguration = federationConfiguration;

        compileRoutingPatterns = new MemoizedFunction<>((spec, lastCompiledPatterns) -> {
            logger.info("Detected new routing rules, compiling them");
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
     * Iterate each of the regular expressions in the order they are defined, and returns the first match it encounters.
     * If no match, defaults to {@link CellInfoResolver#getDefault()}.
     */
    @Override
    public Cell routeKey(String key) {
        Map<Cell, Pattern> cellRoutingPatterns = compileRoutingPatterns.apply(federationConfiguration.getRoutingRules());
        return cellRoutingPatterns.entrySet().stream()
                .filter(entry -> entry.getValue().matcher(key).matches())
                .findFirst()
                .map(Map.Entry::getKey)
                .orElseGet(cellInfoResolver::getDefault);
    }

}
