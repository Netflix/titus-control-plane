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

import java.util.regex.PatternSyntaxException;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class DefaultCellRouterTest {
    private static Logger logger = LoggerFactory.getLogger(DefaultCellRouterTest.class);

    @Test
    public void cellRoutingRulesFromConfig() {
        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("cell1=(app1.*|app2.*);cell2=(app3.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);
        DefaultCellRouter cellRouter = new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration);

        assertThat(cellRouter.routeKey("app3foobar").getName()).isEqualTo("cell2");
        assertThat(cellRouter.routeKey("app2foobar").getName()).isEqualTo("cell1");
        // if not rules, by default go to the first configured in titus.federation.cells
        assertThat(cellRouter.routeKey("other").getName()).isEqualTo("cell1");
    }

    @Test
    public void cellsWithNoRulesCanExist() {
        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("cell1=(app1.*|app2.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);

        DefaultCellRouter cellRouter = new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration);
        Cell cell = cellRouter.routeKey("app2foobar");
        assertThat(cell.getName()).isEqualTo("cell1");
    }

    @Test
    public void rulesWithNonConfiguredCellsAreIgnored() {
        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("cell1=(app1.*|app2.*);cell2=(app3.*);cell3=(app4.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);

        DefaultCellRouter cellRouter = new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration);
        Cell cell = cellRouter.routeKey("app4foobar");
        assertThat(cell.getName()).isEqualTo("cell1"); // no rules default to first
    }

    @Test
    public void invalidInitialRoutingPatternThrowsException() {
        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("cell1=(app1.*|app2.*);cell2=#)(");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);

        assertThatThrownBy(() -> new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration))
                .isInstanceOf(PatternSyntaxException.class);
    }

    @Test
    public void rulesCanBeChangedDynamically() {
        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("cell1=(app1.*|app2.*);cell2=(app3.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);
        DefaultCellRouter cellRouter = new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration);

        assertThat(cellRouter.routeKey("app3foobar").getName()).isEqualTo("cell2");
        assertThat(cellRouter.routeKey("app2foobar").getName()).isEqualTo("cell1");
        // if not rules, by default go to the first configured in titus.federation.cells
        assertThat(cellRouter.routeKey("other").getName()).isEqualTo("cell1");

        // flip rules
        reset(titusFederationConfiguration);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("cell2=(app1.*|app2.*);cell1=(app3.*)");

        assertThat(cellRouter.routeKey("app3foobar").getName()).isEqualTo("cell1");
        assertThat(cellRouter.routeKey("app2foobar").getName()).isEqualTo("cell2");
        // if not rules, by default go to the first configured in titus.federation.cells
        assertThat(cellRouter.routeKey("other").getName()).isEqualTo("cell1");
    }
}
