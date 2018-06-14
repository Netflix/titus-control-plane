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


import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.CollectionsExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class CellInfoUtil {
    private static final Logger logger = LoggerFactory.getLogger(CellInfoUtil.class);

    private static final String CELL_HOST_DELIM = ";";
    private static final String CELL_HOST_RULE_DELIM = "=";

    private static final String CELL_ROUTE_DELIM = ";";
    private static final String CELL_ROUTE_RULE_DELIM = "=";

    /**
     * Extracts cells from a cell specification that takes the following form
     * <tt>cell1=hostName1:7001;cell2=hostName2:7002</tt>
     *
     * <tt>cellSpecification</tt> must contain one entry for each cell. Only the first configuration for each cell name
     * is kept.
     *
     * @return {@link Cell cells} indexed by their name
     */
    static List<Cell> extractCellsFromCellSpecification(String cellsSpecification) {
        return Arrays.stream(cellsSpecification.split(CELL_HOST_DELIM))
                .filter(cellSpec -> cellSpec.contains(CELL_HOST_RULE_DELIM))
                .map(cellSpec -> cellSpec.split(CELL_HOST_RULE_DELIM))
                .map(parts -> new Cell(parts[0], parts[1]))
                .collect(Collectors.toList());
    }

    /**
     * Extract cell routing configurations that take the following form <tt>cell1=someRuleString;cell2=someOtherRuleString</tt>.
     * <p>
     * Iteration on the returned {@link Map} will respect the order entries appear in the <tt>cellRoutingSpecification</tt>
     * argument.
     */
    static Map<Cell, String> extractCellRoutingFromCellSpecification(List<Cell> cells, String cellRoutingSpecification) {
        Map<String, Cell> cellsByName = CollectionsExt.indexBy(cells, Cell::getName);

        return Arrays.stream(cellRoutingSpecification.split(CELL_ROUTE_DELIM))
                .filter(cellRoutingSpec -> cellRoutingSpec.contains(CELL_ROUTE_RULE_DELIM))
                .map(CellRoutingSpec::new)
                .filter(spec -> isCellConfigured(cellsByName, spec))
                .collect(Collectors.toMap(
                        spec -> cellsByName.get(spec.getCellName()),
                        CellRoutingSpec::getRoutingRule,
                        CellInfoUtil::mergeKeepingFirst,
                        LinkedHashMap::new
                ));
    }

    private static boolean isCellConfigured(Map<String, Cell> cellsByName, CellRoutingSpec spec) {
        boolean isCellConfigured = cellsByName.containsKey(spec.getCellName());
        if (!isCellConfigured) {
            logger.error("Found a rule {} for cell {} that is not configured, ignoring it", spec.getRoutingRule(),
                    spec.getCellName());
        }
        return isCellConfigured;
    }

    private static <V> V mergeKeepingFirst(V v1, V v2) {
        logger.warn("Duplicate values found, the second will be ignored: {} and {}", v1, v2);
        return v1;
    }

    private static final class CellRoutingSpec {
        private final String cellName;
        private final String routingRule;

        private CellRoutingSpec(String cellRoutingSpec) {
            String[] parts = cellRoutingSpec.split(CELL_ROUTE_RULE_DELIM);
            this.cellName = parts[0];
            this.routingRule = parts[1];
        }

        private CellRoutingSpec(String cellName, String routingRule) {
            this.cellName = cellName;
            this.routingRule = routingRule;
        }

        private String getCellName() {
            return cellName;
        }

        private String getRoutingRule() {
            return routingRule;
        }
    }
}
