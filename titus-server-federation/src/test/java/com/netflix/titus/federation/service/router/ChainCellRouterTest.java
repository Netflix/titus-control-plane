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

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class ChainCellRouterTest {

    private static final Cell CELL_1 = new Cell("cell1", "cell.1");
    private static final Cell CELL_2 = new Cell("cell2", "cell.2");

    @Test
    public void testChaining() {
        CellRouter first = Mockito.mock(CellRouter.class);
        CellRouter second = Mockito.mock(CellRouter.class);
        ChainCellRouter chain = new ChainCellRouter(asList(first, second));

        // Nothing matches
        when(first.routeKey(any())).thenReturn(Optional.empty());
        when(second.routeKey(any())).thenReturn(Optional.empty());
        assertThat(chain.routeKey(JobDescriptor.getDefaultInstance())).isEmpty();

        // First cell matches
        when(first.routeKey(any())).thenReturn(Optional.of(CELL_1));
        when(second.routeKey(any())).thenReturn(Optional.of(CELL_2));
        assertThat(chain.routeKey(JobDescriptor.getDefaultInstance())).contains(CELL_1);

        // Second cell matches
        when(first.routeKey(any())).thenReturn(Optional.empty());
        when(second.routeKey(any())).thenReturn(Optional.of(CELL_2));
        assertThat(chain.routeKey(JobDescriptor.getDefaultInstance())).contains(CELL_2);
    }
}