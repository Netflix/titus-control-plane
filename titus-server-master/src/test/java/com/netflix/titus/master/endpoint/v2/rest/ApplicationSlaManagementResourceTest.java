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
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.endpoint.rest.ErrorResponse;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
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

    private static final ParameterizedTypeReference<List<ApplicationSlaRepresentation>> APPLICATION_SLA_REP_LIST_TR =
            new ParameterizedTypeReference<List<ApplicationSlaRepresentation>>() {
            };
    private static final ParameterizedTypeReference<List<ApplicationSlaRepresentation>> APPLICATION_SLA_KUBE_SCHEDULER_REP_LIST_TR =
            new ParameterizedTypeReference<List<ApplicationSlaRepresentation>>() {
            };

    private static final ApplicationSLA SAMPLE_SLA = ApplicationSlaSample.CriticalSmall.build();
    private static final ApplicationSLA SAMPLE_SLA_MANAGED_BY_KUBESCHEDULER = ApplicationSlaSample.CriticalSmallKubeScheduler.build();
    private static final ApplicationSlaRepresentation SAMPLE_SLA_REPRESENTATION = Representation2ModelConvertions.asRepresentation(SAMPLE_SLA);

    private static final ApplicationSlaManagementService capacityManagementService = mock(ApplicationSlaManagementService.class);

    private static ReadOnlyJobOperations jobOperations = mock(ReadOnlyJobOperations.class);

    private static final ApplicationSlaManagementResource restService = new ApplicationSlaManagementResource(
            Archaius2Ext.newConfiguration(MasterConfiguration.class),
            capacityManagementService,
            new ReservationUsageCalculator(jobOperations, capacityManagementService)
    );

    @ClassRule
    public static final JaxRsServerResource<ApplicationSlaManagementResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static String baseURI;
    private static WebTestClient testClient;

    @BeforeClass
    public static void setUpClass() {
        when(jobOperations.getJobsAndTasks()).thenReturn(Collections.emptyList());

        baseURI = jaxRsServer.getBaseURI() + ApplicationSlaManagementEndpoint.PATH_API_V2_MANAGEMENT_APPLICATIONS + '/';
        testClient = WebTestClient.bindToServer()
                .baseUrl(baseURI)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .build();
    }

    @Before
    public void setUp() throws Exception {
        Mockito.reset(capacityManagementService);
    }

    @Test
    public void addApplication() {
        when(capacityManagementService.getApplicationSLA(any())).thenReturn(null);
        when(capacityManagementService.addApplicationSLA(any())).thenReturn(Observable.empty());

        testClient.post().body(BodyInserters.fromObject(SAMPLE_SLA_REPRESENTATION)).exchange()
                .expectStatus().is2xxSuccessful()
                .expectHeader().valueEquals(HttpHeaders.LOCATION, baseURI + SAMPLE_SLA.getAppName())
                .expectBody().isEmpty();

        verify(capacityManagementService, times(1)).getApplicationSLA(any());
        verify(capacityManagementService, times(1)).addApplicationSLA(any());
    }

    @Test
    public void addExistingApplicationFails() {
        when(capacityManagementService.getApplicationSLA(any())).thenReturn(SAMPLE_SLA);

        testClient.post().body(BodyInserters.fromObject(SAMPLE_SLA_REPRESENTATION)).exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT);

        verify(capacityManagementService, times(1)).getApplicationSLA(any());
        verify(capacityManagementService, times(0)).addApplicationSLA(any());
    }

    @Test
    public void getAllApplications() {
        when(capacityManagementService.getApplicationSLAs()).thenReturn(Arrays.asList(
                ApplicationSlaSample.CriticalSmall.build(), ApplicationSlaSample.CriticalLarge.build()
        ));

        testClient.get().exchange()
                .expectStatus().isOk()
                .expectBody(APPLICATION_SLA_REP_LIST_TR).value(result -> assertThat(result).hasSize(2));

        verify(capacityManagementService, times(1)).getApplicationSLAs();
    }

    @Test
    public void getApplicationSlaBySchedulerName() {
        when(capacityManagementService.getApplicationSLAsForScheduler("kubescheduler")).thenReturn(Arrays.asList(SAMPLE_SLA_MANAGED_BY_KUBESCHEDULER));
        testClient.get()
                .uri(uriBuilder -> uriBuilder.queryParam("schedulerName", "kubescheduler").build())
                .exchange()
                .expectStatus().isOk()
                .expectBody(APPLICATION_SLA_KUBE_SCHEDULER_REP_LIST_TR).value(result -> assertThat(result).hasSize(1));
        verify(capacityManagementService, times(1)).getApplicationSLAsForScheduler("kubescheduler");
    }

    @Test
    public void getApplicationByName() {
        String targetName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetName)).thenReturn(SAMPLE_SLA);

        testClient.get().uri(targetName).exchange()
                .expectStatus().isOk()
                .expectBody(ApplicationSlaRepresentation.class).value(result -> assertThat(result.getAppName()).isEqualTo(targetName));

        verify(capacityManagementService, times(1)).getApplicationSLA(targetName);
    }

    @Test
    public void getNonExistingApplicationByNameFails() {
        String myMissingApp = "myMissingApp";
        when(capacityManagementService.getApplicationSLA(any())).thenReturn(null);

        testClient.get().uri(myMissingApp).exchange()
                .expectStatus().isNotFound();

        verify(capacityManagementService, times(1)).getApplicationSLA(myMissingApp);
    }

    @Test
    public void updateApplication() {
        String targetName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetName)).thenReturn(SAMPLE_SLA);
        when(capacityManagementService.addApplicationSLA(any())).thenReturn(Observable.empty());

        testClient.put().uri(targetName).body(BodyInserters.fromObject(SAMPLE_SLA_REPRESENTATION)).exchange()
                .expectStatus().isNoContent()
                .expectBody().isEmpty();

        verify(capacityManagementService, times(1)).getApplicationSLA(targetName);
    }

    @Test
    public void updateNonExistingApplicationFails() {
        String targetAppName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetAppName)).thenReturn(null);

        testClient.put().uri(targetAppName).body(BodyInserters.fromObject(SAMPLE_SLA_REPRESENTATION)).exchange()
                .expectStatus().isNotFound()
                .expectBody(ErrorResponse.class).value(error -> assertThat(error.getMessage()).contains("SLA not defined for"));

        verify(capacityManagementService, times(1)).getApplicationSLA(targetAppName);
        verify(capacityManagementService, never()).addApplicationSLA(any());
    }

    @Test
    public void removeApplication() {
        String targetAppName = SAMPLE_SLA.getAppName();
        when(capacityManagementService.getApplicationSLA(targetAppName)).thenReturn(SAMPLE_SLA);
        when(capacityManagementService.removeApplicationSLA(targetAppName)).thenReturn(Observable.empty());

        testClient.delete().uri(targetAppName).exchange()
                .expectStatus().isNoContent();

        verify(capacityManagementService, times(1)).removeApplicationSLA(targetAppName);
    }

    @Test
    public void removeNonExistingApplicationFails() {
        when(capacityManagementService.getApplicationSLA(anyString())).thenReturn(null);

        testClient.delete().uri("/myMissingApp").exchange()
                .expectStatus().isNotFound();

        verify(capacityManagementService, times(1)).getApplicationSLA(any());
        verify(capacityManagementService, never()).removeApplicationSLA(any());
    }
}