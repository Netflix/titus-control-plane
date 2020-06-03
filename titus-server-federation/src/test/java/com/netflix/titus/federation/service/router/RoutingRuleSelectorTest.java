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

import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.federation.service.CellInfoResolver;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoutingRuleSelectorTest {

    private static final Cell CELL_1 = new Cell("cell1", "cell.1");
    private static final Cell CELL_2 = new Cell("cell2", "cell.2");

    private final CellInfoResolver cellInfoResolver = mock(CellInfoResolver.class);

    @Before
    public void setUp() {
        when(cellInfoResolver.resolve()).thenReturn(asList(CELL_1, CELL_2));
    }

    @Test(expected = NullPointerException.class)
    public void testFailFast() {
        new RoutingRuleSelector(cellInfoResolver, () -> null);
    }

    @Test
    public void testParser() {
        RoutingRuleSelector selector = new RoutingRuleSelector(cellInfoResolver, () -> "cell1=(app1.*);cell2=(app2.*)");
        assertThat(selector.select("app1", cell -> true)).contains(CELL_1);
        assertThat(selector.select("app2", cell -> true)).contains(CELL_2);
    }

    @Test
    public void testReload() {
        AtomicReference<String> rules = new AtomicReference<>("cell1=(app1.*);cell2=(app2.*)");
        RoutingRuleSelector selector = new RoutingRuleSelector(cellInfoResolver, rules::get);
        assertThat(selector.select("app1", cell -> true)).contains(CELL_1);
        rules.set("cell1=(app2.*);cell2=(app1.*)");
        assertThat(selector.select("app1", cell -> true)).contains(CELL_2);
    }

    @Test
    public void testReloadFailure() {
        AtomicReference<String> rules = new AtomicReference<>("cell1=(app1.*);cell2=(app2.*)");
        RoutingRuleSelector selector = new RoutingRuleSelector(cellInfoResolver, rules::get);
        assertThat(selector.select("app1", cell -> true)).contains(CELL_1);

        rules.set(null);
        assertThat(selector.select("app1", cell -> true)).contains(CELL_1);

        rules.set("cell1=(app2.*);cell2=(app1.*)");
        assertThat(selector.select("app1", cell -> true)).contains(CELL_2);
    }
}