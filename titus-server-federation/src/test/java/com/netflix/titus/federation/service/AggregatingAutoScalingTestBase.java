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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.grpc.AnonymousSessionContext;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Rule;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AggregatingAutoScalingTestBase {
    static final String POLICY_1 = "policy1";
    static final String POLICY_2 = "policy2";
    static final String JOB_1 = "job1";
    static final String JOB_2 = "job2";

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();

    AggregatingAutoScalingService service;

    @Before
    public void setUp() {
        CellConnector connector = mock(CellConnector.class);
        Map<Cell, ManagedChannel> cellMap = new HashMap<>();
        cellMap.put(new Cell("one", "1"), cellOne.getChannel());
        cellMap.put(new Cell("two", "2"), cellTwo.getChannel());
        when(connector.getChannels()).thenReturn(cellMap);
        when(connector.getChannelForCell(any())).then(invocation ->
                Optional.ofNullable(cellMap.get(invocation.getArgument(0)))
        );

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(1000L);
        service = new AggregatingAutoScalingService(connector, new AnonymousSessionContext(), grpcConfiguration);
    }

}
