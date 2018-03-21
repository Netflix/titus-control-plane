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

import io.netflix.titus.api.endpoint.v2.rest.representation.LogLinksRepresentation;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.master.endpoint.v2.V2LegacyTitusServiceGateway;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import io.netflix.titus.testkit.junit.jaxrs.HttpTestClient;
import io.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogsResourceTest {

    private static final V2LegacyTitusServiceGateway serviceGateway = mock(V2LegacyTitusServiceGateway.class);

    private static final LogsResource restService = new LogsResource(serviceGateway);

    @ClassRule
    public static final JaxRsServerResource<LogsResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static HttpTestClient client;

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator(getClass().getSimpleName());

    @BeforeClass
    public static void setUpClass() throws Exception {
        client = new HttpTestClient(jaxRsServer.getBaseURI());
    }

    @Before
    public void setUp() throws Exception {
        Mockito.reset(serviceGateway);
    }

    @Test
    public void getLogsByTaskId() throws Exception {
        String jobId = generator.newJobInfo(TitusJobType.batch, "myJob").getId();
        generator.scheduleJob(jobId);
        TitusTaskInfo titusTaskInfo = generator.getTitusTaskInfos(jobId).get(0);

        when(serviceGateway.findTaskById(any())).thenReturn(Observable.just(titusTaskInfo));
        LogLinksRepresentation reply = client.doGET("/api/v2/logs/" + titusTaskInfo.getId(), LogLinksRepresentation.class);

        verify(serviceGateway, times(1)).findTaskById(titusTaskInfo.getId());
    }
}