/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.federation.endpoint.rest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.federation.endpoint.EndpointConfiguration;
import com.netflix.titus.federation.service.CellWebClientConnector;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.web.reactive.function.client.WebClient;

import static com.netflix.titus.federation.endpoint.rest.FederationV2CapacityGroupResource.API_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class FederationV2CapacityGroupResourceTest {

    private static final ApplicationSLA CELL_1_CAPACITY_GROUP = ApplicationSlaSample.CriticalLarge.build();
    private static final ApplicationSLA CELL_2_CAPACITY_GROUP = ApplicationSlaSample.CriticalSmall.build();

    private static final Cell CELL_1 = new Cell("cell1", "hostname1:7001");
    private static final Cell CELL_2 = new Cell("cell2", "hostname1:7001");

    private MockWebServer cell1Server = new MockWebServer();
    private MockWebServer cell2Server = new MockWebServer();


    private final CellWebClientConnector webClientConnector = Mockito.mock(CellWebClientConnector.class);

    private final FederationV2CapacityGroupResource resource = new FederationV2CapacityGroupResource(
            Archaius2Ext.newConfiguration(EndpointConfiguration.class),
            webClientConnector
    );

    @Before
    public void setUp() throws Exception {

        cell1Server.setDispatcher(newDispatcher(CELL_1_CAPACITY_GROUP));
        cell2Server.setDispatcher(newDispatcher(CELL_2_CAPACITY_GROUP));
        cell1Server.start();
        cell2Server.start();

        Map<Cell, WebClient> cellWebClients = ImmutableMap.of(
                CELL_1, WebClient.builder().baseUrl(cell1Server.url("/").toString()).build(),
                CELL_2, WebClient.builder().baseUrl(cell1Server.url("/").toString()).build()
        );
        when(webClientConnector.getWebClients()).thenReturn(cellWebClients);
    }

    @After
    public void tearDown() {
        ExceptionExt.silent(() -> cell1Server.shutdown());
        ExceptionExt.silent(() -> cell2Server.shutdown());
    }

    @Test
    public void testGetCapacityGroup() {
        ApplicationSlaRepresentation result = resource.getApplicationSLA(CELL_1_CAPACITY_GROUP.getAppName(), false);
        assertThat(result).isNotNull();
    }

    @Test
    public void testGetCapacityGroupNotFound() {
        try {
            resource.getApplicationSLA("myFakeCapacityGroup", false);
        } catch (WebApplicationException e) {
            assertThat(e.getResponse().getStatus()).isEqualTo(404);
        }
    }

    @Test
    public void testGetCapacityGroupSystemError() throws IOException {
        cell1Server.shutdown();
        try {
            resource.getApplicationSLA("myFakeCapacityGroup", false);
        } catch (WebApplicationException e) {
            assertThat(e.getResponse().getStatus()).isEqualTo(500);
        }
    }

    @Test
    public void testGetCapacityGroups() {
        List<ApplicationSlaRepresentation> result = resource.getApplicationSLAs(false);
        assertThat(result).isNotNull();
        assertThat(result).hasSize(2);
    }

    @Test
    public void testGetCapacityGroupsSystemError() throws IOException {
        cell1Server.shutdown();
        try {
            resource.getApplicationSLAs(false);
        } catch (WebApplicationException e) {
            assertThat(e.getResponse().getStatus()).isEqualTo(500);
        }
    }

    private Dispatcher newDispatcher(ApplicationSLA capacityGroup) {
        return new Dispatcher() {

            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                if (request.getPath().equals(API_PATH + "?includeUsage=false")) {
                    return newMockGetResult(Collections.singletonList(capacityGroup));
                }
                if (request.getPath().equals(API_PATH + '/' + capacityGroup.getAppName() + "?includeUsage=false")) {
                    return newMockGetResult(capacityGroup);
                }
                return new MockResponse().setResponseCode(404);
            }
        };
    }

    private <T> MockResponse newMockGetResult(T body) {
        try {
            return new MockResponse()
                    .setResponseCode(200)
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .setBody(ObjectMappers.storeMapper().writeValueAsString(body));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}