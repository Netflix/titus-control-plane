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

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.Container;
import com.netflix.titus.grpc.protogen.ContainerResources;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpecialInstanceTypeRouterTest {

    private static final Cell CELL_1 = new Cell("cell1", "cell.1");
    private static final Cell CELL_2 = new Cell("cell2", "cell.2");

    private final CellInfoResolver cellInfoResolver = mock(CellInfoResolver.class);

    private final TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);

    private CellRouter router;

    @Before
    public void setUp() {
        when(cellInfoResolver.resolve()).thenReturn(asList(CELL_1, CELL_2));
        when(titusFederationConfiguration.getInstanceTypeRoutingRules()).thenReturn("cell2=(gpu.*)");
        router = SpecialInstanceTypeRouter.getGpuInstanceTypeRouter(cellInfoResolver, titusFederationConfiguration);
    }

    @Test
    public void testGpuCellIsSelectedForGpuJob() {
        JobDescriptor gpuJobDescriptor = JobDescriptor.newBuilder()
                .setContainer(Container.newBuilder()
                        .setResources(ContainerResources.newBuilder()
                                .setGpu(5)
                        )
                )
                .build();
        assertThat(router.routeKey(gpuJobDescriptor)).contains(CELL_2);
    }

    @Test
    public void testNoCellIsSelectedForNonGpuJob() {
        assertThat(router.routeKey(JobDescriptor.getDefaultInstance())).isEmpty();
    }
}