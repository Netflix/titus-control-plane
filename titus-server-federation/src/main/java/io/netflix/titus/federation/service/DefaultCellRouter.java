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

package io.netflix.titus.federation.service;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.federation.startup.TitusFederationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultCellRouter implements CellRouter {
    private static Logger logger = LoggerFactory.getLogger(DefaultCellRouter.class);
    private final SortedMap<Cell, Pattern> cellRoutingPatterns;

    @Inject
    public DefaultCellRouter(CellInfoResolver cellInfoResolver, TitusFederationConfiguration federationConfiguration) {
        List<Cell> cells = cellInfoResolver.resolve();
        Map<Cell, String> cellRoutingRules = CellInfoUtil.extractCellRoutingFromCellSpecification(cells, federationConfiguration.getRoutingRules());
        SortedMap<Cell, Pattern> sortedPatternMap = new TreeMap<>();
        cellRoutingRules.forEach((cell, routingRule) -> sortedPatternMap.putIfAbsent(cell, Pattern.compile(routingRule)));
        cellRoutingPatterns = sortedPatternMap;
    }

    /**
     * routeKey iterates each of the regular expressions in cell order to and returns
     * the first match it encounters. If no match, default to first cell.
     * @param key
     * @return
     */
    @Override
    public Cell routeKey(String key) {
        // Iterate the cell patterns in sorted order
        for (Map.Entry<Cell, Pattern> entry : cellRoutingPatterns.entrySet()) {
            Cell cell = entry.getKey();
            Pattern cellPattern = entry.getValue();
            if (cellPattern.matcher(key).matches()) {
                return cell;
            }
        }
        return cellRoutingPatterns.firstKey();
    }
}
