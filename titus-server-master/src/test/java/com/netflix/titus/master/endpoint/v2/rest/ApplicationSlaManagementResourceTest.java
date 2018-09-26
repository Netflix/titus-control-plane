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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.api.endpoint.v2.rest.representation.TypeReferences;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.jaxrs.HttpTestClient;
import com.netflix.titus.testkit.junit.jaxrs.HttpTestClientException;
import com.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Runs tests in the embedded jetty jaxRsServer, as we want to verify that providers and annotations are applied
 * as expected.
 */
@Category(IntegrationTest.class)
public class ApplicationSlaManagementResourceTest {

    private static final ApplicationSLA SAMPLE_SLA = ApplicationSlaSample.CriticalSmall.build();
    private static final ApplicationSlaRepresentation SAMPLE_SLA_REPRESENTATION = representationOf(SAMPLE_SLA);

    private static final ApplicationSlaManagementService capacityManagementService = mock(ApplicationSlaManagementService.class);

    private static final ApplicationSlaManagementResource restService = new ApplicationSlaManagementResource(capacityManagementService);

    @ClassRule
    public static final JaxRsServerResource<ApplicationSlaManagementResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static HttpTestClient client;

    @BeforeClass
    public static void setUpClass() throws Exception {
        client = new HttpTestClient(
                jaxRsServer.getBaseURI() + ApplicationSlaManagementEndpoint.PATH_API_V2_MANAGEMENT_APPLICATIONS
        );
    }

    @Before
    public void setUp() throws Exception {
        Mockito.reset(capacityManagementService);
    }

    @Test
    public void addApplication() throws Exception {
        when(capacityManagementService.getApplicationSLA(any())).thenReturn(null);
        when(capacityManagementService.addApplicationSLA(any())).thenReturn(Observable.empty());

        client.doPOST(SAMPLE_SLA_REPRESENTATION, SAMPLE_SLA.getAppName());

        verify(capacityManagementService, times(1)).getApplicationSLA(any());
        verify(capacityManagementService, times(1)).addApplicationSLA(any());
    }

    @Test
    public void addExistingApplicationFails() throws Exception {
        when(capacityManagementService.getApplicationSLA(any())).thenReturn(SAMPLE_SLA);

        execute(() -> client.doPOST(SAMPLE_SLA_REPRESENTATION, SAMPLE_SLA.getAppName()), 409);

        verify(capacityManagementService, times(1)).getApplicationSLA(any());
        verify(capacityManagementService, times(0)).addApplicationSLA(any());
    }

    @Test
    public void getAllApplications() throws Exception {
        when(capacityManagementService.getApplicationSLAs()).thenReturn(Arrays.asList(
                ApplicationSlaSample.CriticalSmall.build(), ApplicationSlaSample.CriticalLarge.build()
        ));

        List<ApplicationSlaRepresentation> result = client.doGET(TypeReferences.APPLICATION_SLA_REP_LIST_TREF);
        assertThat(result).hasSize(2);

        verify(capacityManagementService, times(1)).getApplicationSLAs();
    }

    @Test
    public void getApplicationByName() throws Exception {
        String targetName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetName)).thenReturn(SAMPLE_SLA);

        ApplicationSlaRepresentation result = client.doGET('/' + targetName, ApplicationSlaRepresentation.class);
        assertThat(result.getAppName()).isEqualTo(targetName);

        verify(capacityManagementService, times(1)).getApplicationSLA(targetName);
    }

    @Test
    public void getNonExistingApplicationByNameFails() throws Exception {
        String myMissingApp = "myMissingApp";
        when(capacityManagementService.getApplicationSLA(any())).thenReturn(null);

        execute(() -> client.doGET("/" + myMissingApp, ApplicationSlaRepresentation.class), 404);
        verify(capacityManagementService, times(1)).getApplicationSLA(myMissingApp);
    }

    @Test
    public void updateApplication() throws Exception {
        String targetName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetName)).thenReturn(SAMPLE_SLA);
        when(capacityManagementService.addApplicationSLA(any())).thenReturn(Observable.empty());

        client.doPUT('/' + targetName, SAMPLE_SLA_REPRESENTATION);

        verify(capacityManagementService, times(1)).getApplicationSLA(targetName);
    }

    @Test
    public void updateNonExistingApplicationFails() throws Exception {
        String targetAppName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetAppName)).thenReturn(null);

        execute(() -> client.doPUT('/' + targetAppName, SAMPLE_SLA_REPRESENTATION), 404);

        verify(capacityManagementService, times(1)).getApplicationSLA(targetAppName);
        verify(capacityManagementService, never()).addApplicationSLA(any());
    }

    @Test
    public void removeApplication() throws Exception {
        String targetAppName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetAppName)).thenReturn(SAMPLE_SLA);
        when(capacityManagementService.removeApplicationSLA(targetAppName)).thenReturn(Observable.empty());

        client.doDELETE('/' + targetAppName);

        verify(capacityManagementService, times(1)).removeApplicationSLA(targetAppName);
    }

    @Test
    public void removeNonExistingApplicationFails() throws Exception {
        when(capacityManagementService.getApplicationSLA(anyString())).thenReturn(null);

        execute(() -> client.doDELETE("/myMissingApp"), 404);

        verify(capacityManagementService, times(1)).getApplicationSLA(any());
        verify(capacityManagementService, never()).removeApplicationSLA(any());
    }

    private static ApplicationSlaRepresentation representationOf(ApplicationSLA sample) {
        return Representation2ModelConvertions.asRepresentation(sample);
    }

    private void execute(Action httpReq, int statusCode) throws Exception {
        Preconditions.checkArgument(statusCode / 100 != 2);
        try {
            httpReq.call();
            fail("HTTP request succeeded, while it has been expected to fail");
        } catch (HttpTestClientException e) {
            assertThat(e.getStatusCode()).isEqualTo(statusCode);
        }
    }

    interface Action {
        void call() throws Exception;
    }
}