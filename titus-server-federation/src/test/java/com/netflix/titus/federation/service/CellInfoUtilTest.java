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


import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.federation.model.Cell;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CellInfoUtilTest {

    @Test
    public void checkTwoCellSpecifications() {
        String cellsSpec = "cell1=hostName1:7001;cell2=hostName2:7002";
        List<Cell> cells = CellInfoUtil.extractCellsFromCellSpecification(cellsSpec);
        assertThat(cells).isNotNull();
        assertThat(cells.size()).isEqualTo(2);
        assertThat(cells.get(0).getAddress()).isEqualTo("hostName1:7001");
        assertThat(cells.get(1).getAddress()).isEqualTo("hostName2:7002");
    }

    @Test
    public void checkSingleCellSpecifications() {
        String cellsSpec = "cell1=hostName1:7001";
        List<Cell> cells = CellInfoUtil.extractCellsFromCellSpecification(cellsSpec);
        assertThat(cells).isNotNull();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(cells.get(0).getAddress()).isEqualTo("hostName1:7001");
    }

    @Test
    public void checkEmptyCellSpecifications() {
        String cellsSpec = "";
        List<Cell> cells = CellInfoUtil.extractCellsFromCellSpecification(cellsSpec);
        assertThat(cells).isNotNull();
        assertThat(cells.size()).isEqualTo(0);
    }

    @Test
    public void duplicatedRoutingRulesAreIgnored() {
        Cell cell = new Cell("cell1", "1");
        String cellsSpec = "cell1=(app1.*);cell1=(app2.*)";
        Map<Cell, String> routingRules = CellInfoUtil.extractCellRoutingFromCellSpecification(
                Collections.singletonList(cell), cellsSpec
        );
        assertThat(routingRules).isNotNull();
        assertThat(routingRules).hasSize(1);
        // second value got ignored
        assertThat(routingRules.get(cell)).isEqualTo("(app1.*)");
    }
}
