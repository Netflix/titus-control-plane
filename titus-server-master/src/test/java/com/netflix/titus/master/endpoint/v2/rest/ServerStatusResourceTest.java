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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.Collections;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.api.endpoint.v2.rest.representation.ServerStatusRepresentation;
import com.netflix.titus.common.util.guice.ActivationLifecycle;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.health.service.DefaultHealthService;
import com.netflix.titus.master.supervisor.service.LeaderActivator;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.web.reactive.server.WebTestClient;

import static com.netflix.titus.master.endpoint.v2.rest.ServerStatusResource.NOT_APPLICABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class ServerStatusResourceTest {

    private static final CellInfoResolver cellInfoResolver = mock(CellInfoResolver.class);

    private static final ActivationLifecycle activationLifecycle = mock(ActivationLifecycle.class);

    private static final LeaderActivator leaderActivator = mock(LeaderActivator.class);

    private static final ServerStatusResource restService = new ServerStatusResource(
            new DefaultHealthService(cellInfoResolver, activationLifecycle, leaderActivator)
    );

    @ClassRule
    public static final JaxRsServerResource<ServerStatusResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static WebTestClient testClient;

    @BeforeClass
    public static void setUpClass() {
        testClient = WebTestClient.bindToServer()
                .baseUrl(jaxRsServer.getBaseURI())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .build();
    }

    @Before
    public void setUp() throws Exception {
        reset(cellInfoResolver, activationLifecycle, leaderActivator);
        when(cellInfoResolver.getCellName()).thenReturn("cellN");
    }

    @Test
    public void testActivatedServer() {
        when(leaderActivator.isLeader()).thenReturn(true);
        when(leaderActivator.isActivated()).thenReturn(true);
        when(activationLifecycle.getServiceActionTimesMs()).thenReturn(Collections.singletonList(Pair.of("myService", 90L)));

        testClient.get().uri(ServerStatusResource.PATH_API_V2_STATUS).exchange()
                .expectBody(ServerStatusRepresentation.class)
                .value(result -> {
                    assertThat(result.isLeader()).isTrue();
                    assertThat(result.isActive()).isTrue();
                    assertThat(result.getServiceActivationTimes()).hasSize(1);
                    assertThat(result.getServiceActivationTimes().get(0).getDuration()).isEqualTo("90ms");
                    assertThat(result.getServiceActivationOrder()).hasSize(1);
                });
    }

    @Test
    public void tesNotLeaderServer() {
        testClient.get().uri(ServerStatusResource.PATH_API_V2_STATUS).exchange()
                .expectBody(ServerStatusRepresentation.class)
                .value(result -> {
                    assertThat(result.isLeader()).isFalse();
                    assertThat(result.isActive()).isFalse();
                    assertThat(result.getActivationTime()).isEqualTo(NOT_APPLICABLE);
                    assertThat(result.getServiceActivationTimes()).hasSize(0);
                    assertThat(result.getServiceActivationTimes()).hasSize(0);
                    assertThat(result.getServiceActivationOrder()).hasSize(0);
                });
    }

    @Test
    public void testUnactivatedServer() {
        when(leaderActivator.isLeader()).thenReturn(true);
        when(activationLifecycle.getActivationTimeMs()).thenReturn(-1L);

        testClient.get().uri(ServerStatusResource.PATH_API_V2_STATUS).exchange()
                .expectBody(ServerStatusRepresentation.class)
                .value(result -> {
                    assertThat(result.isLeader()).isTrue();
                    assertThat(result.isActive()).isFalse();
                    assertThat(result.getActivationTime()).isEqualTo(NOT_APPLICABLE);
                    assertThat(result.getServiceActivationTimes()).hasSize(0);
                    assertThat(result.getServiceActivationTimes()).hasSize(0);
                    assertThat(result.getServiceActivationOrder()).hasSize(0);
                });
    }
}