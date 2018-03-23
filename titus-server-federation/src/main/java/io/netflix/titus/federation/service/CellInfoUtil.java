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
package io.netflix.titus.federation.service;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.netflix.titus.api.federation.model.Cell;


public class CellInfoUtil {

    private static final String CELL_HOST_DELIM = ";";
    private static final String CELL_HOST_RULE_DELIM = "=";

    private static final String CELL_ROUTE_DELIM = ";";
    private static final String CELL_ROUTE_RULE_DELIM = "=";

    /*
        Extracts cells from a cell specification that takes the following form
        cell1=hostName1:7001;cell2=hostName2:7002
     */
    public static List<Cell> extractCellsFromCellSpecification(String cellsSpecification) {
        return Arrays.stream(cellsSpecification.split(CELL_HOST_DELIM))
                .filter(cellSpec -> cellSpec.contains(CELL_HOST_RULE_DELIM))
                .map(cellSpec -> {
                    String[] cellParts = cellSpec.split(CELL_HOST_RULE_DELIM);
                    return new Cell(cellParts[0], cellParts[1]);
                })
                .collect(Collectors.toList());
    }

    /**
     * Extract cell routing configurations that take the following form
     * cell1=someRuleString;cell2=someOtherRuleString
     */
    public static Map<Cell, String> extractCellRoutingFromCellSpecification(List<Cell> cells, String cellRoutingSpecification) {
        Map<Cell, String> cellRoutingSpecMap = new HashMap<>();

        List<String> cellRoutingSpecs = Arrays.stream(cellRoutingSpecification.split(CELL_ROUTE_DELIM))
                .filter(cellRoutingSpec -> cellRoutingSpec.contains(CELL_ROUTE_RULE_DELIM))
                .collect(Collectors.toList());

        for (Cell cell : cells) {
            for (String cellRoutingSpec : cellRoutingSpecs) {
                String[] routingParts = cellRoutingSpec.split(CELL_ROUTE_RULE_DELIM);
                String cellName = routingParts[0];
                String routingRule = routingParts[1];
                if (cellName.equals(cell.getName())) {
                    cellRoutingSpecMap.putIfAbsent(cell, routingRule);
                }
            }
        }
        if (cellRoutingSpecMap.keySet().size() != cells.size()) {
            throw CellFederationException.invalidCellConfig(String.format("Cell count of %d does not match routing rule count %d", cells.size(), cellRoutingSpecMap.keySet().size()));
        }
        return cellRoutingSpecMap;
    }
}
