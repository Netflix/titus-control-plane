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

package io.netflix.titus.master.endpoint.v2.rest;

import java.util.Collections;

import io.netflix.titus.api.endpoint.v2.rest.representation.ServerStatusRepresentation;
import io.netflix.titus.common.util.guice.ActivationLifecycle;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.cluster.LeaderActivator;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import io.netflix.titus.testkit.junit.jaxrs.HttpTestClient;
import io.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static io.netflix.titus.master.endpoint.v2.rest.ServerStatusResource.NOT_APPLICABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class ServerStatusResourceTest {

    private static final ActivationLifecycle activationLifecycle = mock(ActivationLifecycle.class);

    private static final LeaderActivator leaderActivator = mock(LeaderActivator.class);

    private static final ServerStatusResource restService = new ServerStatusResource(activationLifecycle, leaderActivator);

    @ClassRule
    public static final JaxRsServerResource<ServerStatusResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static HttpTestClient client;

    @BeforeClass
    public static void setUpClass() {
        client = new HttpTestClient(jaxRsServer.getBaseURI());
    }

    @Before
    public void setUp() throws Exception {
        reset(activationLifecycle, leaderActivator);
    }

    @Test
    public void testActivatedServer() throws Exception {
        when(leaderActivator.isLeader()).thenReturn(true);
        when(leaderActivator.isActivated()).thenReturn(true);
        when(activationLifecycle.getServiceActionTimesMs()).thenReturn(Collections.singletonList(Pair.of("myService", 90L)));

        ServerStatusRepresentation result = client.doGET(ServerStatusResource.PATH_API_V2_STATUS, ServerStatusRepresentation.class);

        assertThat(result.isLeader()).isTrue();
        assertThat(result.isActive()).isTrue();
        assertThat(result.getServiceActivationTimes()).hasSize(1);
        assertThat(result.getServiceActivationTimes().get(0).getDuration()).isEqualTo("90ms");
        assertThat(result.getServiceActivationOrder()).hasSize(1);
    }

    @Test
    public void tesNotLeaderServer() throws Exception {
        ServerStatusRepresentation result = client.doGET(ServerStatusResource.PATH_API_V2_STATUS, ServerStatusRepresentation.class);

        assertThat(result.isLeader()).isFalse();
        assertThat(result.isActive()).isFalse();
        assertThat(result.getActivationTime()).isEqualTo(NOT_APPLICABLE);
        assertThat(result.getServiceActivationTimes()).hasSize(0);
        assertThat(result.getServiceActivationTimes()).hasSize(0);
        assertThat(result.getServiceActivationOrder()).hasSize(0);
    }

    @Test
    public void testUnactivatedServer() throws Exception {
        when(leaderActivator.isLeader()).thenReturn(true);
        when(activationLifecycle.getActivationTimeMs()).thenReturn(-1L);

        ServerStatusRepresentation result = client.doGET(ServerStatusResource.PATH_API_V2_STATUS, ServerStatusRepresentation.class);

        assertThat(result.isLeader()).isTrue();
        assertThat(result.isActive()).isFalse();
        assertThat(result.getActivationTime()).isEqualTo(NOT_APPLICABLE);
        assertThat(result.getServiceActivationTimes()).hasSize(0);
        assertThat(result.getServiceActivationTimes()).hasSize(0);
        assertThat(result.getServiceActivationOrder()).hasSize(0);
    }
}