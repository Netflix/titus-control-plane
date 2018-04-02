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

import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import io.grpc.ManagedChannel;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultCellConnectorTest {

    @Test
    public void buildChannelsTest() {
        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getCells()).thenReturn("cell1=hostName1:7001;cell2=hostName2:7002");

        DefaultCellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);
        DefaultCellConnector defaultCellConnector = new DefaultCellConnector(cellInfoResolver);
        Map<Cell, ManagedChannel> channels = defaultCellConnector.getChannels();

        assertThat(channels).isNotEmpty();
        assertThat(channels.size()).isEqualTo(2);

        Optional<ManagedChannel> hostOneChannel = defaultCellConnector.getChannelForCell(new Cell("cell1", "hostName1:7001"));
        Optional<ManagedChannel> hostTwoChannel = defaultCellConnector.getChannelForCell(new Cell("cell2", "hostName2:7002"));
        assertThat(hostOneChannel.isPresent()).isTrue();
        assertThat(hostTwoChannel.isPresent()).isTrue();


        Optional<ManagedChannel> hostThreeChannel = defaultCellConnector.getChannelForCell(new Cell("cell3", "hostName3:7002"));
        assertThat(hostThreeChannel.isPresent()).isFalse();
    }

}
