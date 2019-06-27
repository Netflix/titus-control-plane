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

package com.netflix.titus.ext.eureka.spring;

import java.net.URI;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.ext.eureka.EurekaServerStub;
import com.netflix.titus.ext.eureka.common.EurekaUris;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

import static com.netflix.titus.ext.eureka.EurekaGenerator.newInstanceInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EurekaLoadBalancingExchangeFilterFunctionTest {

    private static final InstanceInfo INSTANCE_1 = newInstanceInfo("id1", "myservice", "1.0.0.1", InstanceInfo.InstanceStatus.UP);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final EurekaServerStub eurekaServer = new EurekaServerStub();

    @Before
    public void setUp() {
        eurekaServer.register(INSTANCE_1);
        eurekaServer.triggerCacheRefreshUpdate();
    }

    @Test
    public void testInterceptor() {
        ClientResponse response = execute("eureka://myservice:7001");
        assertThat(response.statusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    public void testInterceptorForNonExistingService() {
        ClientResponse response = execute("eureka://wrongservice:7001");
        assertThat(response.statusCode().is5xxServerError()).isTrue();
    }

    private ClientResponse execute(String eurekaUri) {
        EurekaLoadBalancingExchangeFilterFunction filter = new EurekaLoadBalancingExchangeFilterFunction(
                eurekaServer.getEurekaClient(), EurekaUris::getServiceName, titusRuntime
        );

        ClientRequest request = mock(ClientRequest.class);
        when(request.url()).thenReturn(URI.create(eurekaUri));
        when(request.method()).thenReturn(HttpMethod.GET);
        when(request.headers()).thenReturn(HttpHeaders.EMPTY);
        when(request.cookies()).thenReturn(HttpHeaders.EMPTY);

        ExchangeFunction next = mock(ExchangeFunction.class);
        when(next.exchange(any())).thenAnswer(invocation -> {
            ClientRequest rewrittenRequest = invocation.getArgument(0);
            ClientResponse clientResponse = mock(ClientResponse.class);
            if (rewrittenRequest.url().getHost().equals("1.0.0.1")) {
                when(clientResponse.statusCode()).thenReturn(HttpStatus.OK);
            } else {
                when(clientResponse.statusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return Mono.just(clientResponse);
        });

        return filter.filter(request, next).block();
    }
}