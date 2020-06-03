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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.service.CellInfoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RoutingRuleSelector {

    private static final Logger logger = LoggerFactory.getLogger(RoutingRuleSelector.class);

    private final Function<String, Map<Cell, Pattern>> compileRoutingPatterns;
    private final Supplier<String> routingRulesSupplier;

    RoutingRuleSelector(CellInfoResolver cellInfoResolver, Supplier<String> routingRulesSupplier) {
        compileRoutingPatterns = Evaluators.memoizeLast((spec, lastCompiledPatterns) -> {
            logger.info("Detected new routing rules, compiling them: {}", spec);
            try {
                List<Cell> cells = cellInfoResolver.resolve();
                Map<Cell, String> cellRoutingRules = CellInfoUtil.extractCellRoutingFromCellSpecification(cells, spec);
                return CollectionsExt.mapValues(cellRoutingRules, Pattern::compile, LinkedHashMap::new);
            } catch (RuntimeException e) {
                logger.error("Bad cell routing spec, ignoring: {}", spec);
                return lastCompiledPatterns.orElseThrow(() -> e /* there is nothing to do if the first spec is bad */);
            }
        });
        this.routingRulesSupplier = routingRulesSupplier;

        // ensure the initial spec can be compiled or fail fast
        compileRoutingPatterns.apply(routingRulesSupplier.get());
    }

    Optional<Cell> select(String routeKey, Predicate<Cell> filter) {
        Map<Cell, Pattern> cellRoutingPatterns = compileRoutingPatterns.apply(routingRulesSupplier.get());

        return cellRoutingPatterns.entrySet().stream()
                .filter(entry -> filter.test(entry.getKey()))
                .filter(entry -> entry.getValue().matcher(routeKey).matches())
                .findFirst()
                .map(Map.Entry::getKey);
    }
}
