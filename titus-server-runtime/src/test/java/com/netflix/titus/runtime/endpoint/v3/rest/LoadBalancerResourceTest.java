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

package com.netflix.titus.runtime.endpoint.v3.rest;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.service.LoadBalancerService;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.v3.rest.RestConstants.CURSOR_QUERY_KEY;
import static com.netflix.titus.runtime.endpoint.v3.rest.RestConstants.PAGE_QUERY_KEY;
import static com.netflix.titus.runtime.endpoint.v3.rest.RestConstants.PAGE_SIZE_QUERY_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


/**
 * Tests the {@link LoadBalancerResource} class.
 */
public class LoadBalancerResourceTest {
    @Mock
    private LoadBalancerService loadBalancerService;
    @Mock
    private UriInfo uriInfo;
    @Mock
    private SystemLogService systemLog;
    @Mock
    private CallMetadataResolver callMetadataResolver;


    private LoadBalancerResource loadBalancerResource;

    private static final String TEST_JOB_ID = "job-id";
    private static final String TEST_LOAD_BALANCER_ID = "load-balancer_id";

    @Before
    public void beforeAll() {
        MockitoAnnotations.initMocks(this);
        this.loadBalancerResource = new LoadBalancerResource(loadBalancerService, systemLog, callMetadataResolver);
    }

    @Test
    public void getJobLoadBalancersTest() {
        GetJobLoadBalancersResult expectedResult = GetJobLoadBalancersResult.newBuilder().build();
        when(loadBalancerService.getLoadBalancers(any())).thenReturn(Observable.just(expectedResult));

        GetJobLoadBalancersResult actualResult = loadBalancerResource.getJobLoadBalancers(TEST_JOB_ID);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void getAllJobsTest() {
        GetAllLoadBalancersResult expectedResult = GetAllLoadBalancersResult.newBuilder().build();
        when(uriInfo.getQueryParameters()).thenReturn(getDefaultPageParameters());
        when(loadBalancerService.getAllLoadBalancers(any())).thenReturn(Observable.just(expectedResult));

        GetAllLoadBalancersResult actualResult = loadBalancerResource.getAllLoadBalancers(uriInfo);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void addLoadBalancerSucceededTest() {
        when(loadBalancerService.addLoadBalancer(any())).thenReturn(Completable.complete());

        Response response = loadBalancerResource.addLoadBalancer(TEST_JOB_ID, TEST_LOAD_BALANCER_ID);
        assertEquals(200, response.getStatus());
    }

    @Test(expected = TitusServiceException.class)
    public void addLoadBalancerFailedTest() {
        TitusServiceException exception = TitusServiceException.jobNotFound(TEST_JOB_ID);
        when(loadBalancerService.addLoadBalancer(any())).thenReturn(Completable.error(exception));

        loadBalancerResource.addLoadBalancer(TEST_JOB_ID, TEST_LOAD_BALANCER_ID);
    }

    @Test
    public void removeLoadBalancerSucceededTest() {
        when(loadBalancerService.removeLoadBalancer(any())).thenReturn(Completable.complete());

        Response response = loadBalancerResource.removeLoadBalancer(TEST_JOB_ID, TEST_LOAD_BALANCER_ID);
        assertEquals(200, response.getStatus());
    }

    @Test(expected = TitusServiceException.class)
    public void removeLoadBalancerFailedTest() {
        TitusServiceException exception = TitusServiceException.jobNotFound(TEST_JOB_ID);
        when(loadBalancerService.removeLoadBalancer(any())).thenReturn(Completable.error(exception));

        loadBalancerResource.removeLoadBalancer(TEST_JOB_ID, TEST_LOAD_BALANCER_ID);
    }

    private static MultivaluedMap<String, String> getDefaultPageParameters() {
        MultivaluedMap queryParameters = new MultivaluedMapImpl();
        queryParameters.add(PAGE_QUERY_KEY, "0");
        queryParameters.add(PAGE_SIZE_QUERY_KEY, "1");
        queryParameters.add(CURSOR_QUERY_KEY, "");

        return queryParameters;
    }
}
