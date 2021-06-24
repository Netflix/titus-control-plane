/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.federation.startup;

import java.util.Arrays;

import com.netflix.titus.federation.endpoint.FederationEndpointComponent;
import com.netflix.titus.federation.service.AggregatingJobServiceGateway;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.service.DefaultCellConnector;
import com.netflix.titus.federation.service.DefaultCellInfoResolver;
import com.netflix.titus.federation.service.DefaultCellWebClientConnector;
import com.netflix.titus.federation.service.FallbackJobServiceGateway;
import com.netflix.titus.federation.service.JobActivityServiceComponent;
import com.netflix.titus.federation.service.RemoteJobServiceGateway;
import com.netflix.titus.federation.service.ServiceComponent;
import com.netflix.titus.federation.service.SimpleWebClientFactory;
import com.netflix.titus.federation.service.WebClientFactory;
import com.netflix.titus.federation.service.router.ApplicationCellRouter;
import com.netflix.titus.federation.service.router.CellRouter;
import com.netflix.titus.federation.service.router.ChainCellRouter;
import com.netflix.titus.federation.service.router.FallbackCellRouter;
import com.netflix.titus.federation.service.router.SpecialInstanceTypeRouter;
import com.netflix.titus.runtime.TitusEntitySanitizerComponent;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        TitusEntitySanitizerComponent.class,

        DefaultCellInfoResolver.class,
        DefaultCellConnector.class,
        DefaultCellWebClientConnector.class,

        ServiceComponent.class,
        FederationEndpointComponent.class,
        JobActivityServiceComponent.class
})
public class TitusFederationComponent {

    @Bean
    public HostCallerIdResolver getHostCallerIdResolver() {
        return NoOpHostCallerIdResolver.getInstance();
    }

    @Bean
    public WebClientFactory getWebClientFactory() {
        return SimpleWebClientFactory.getInstance();
    }

    @Bean
    public CellRouter getCellRouter(CellInfoResolver cellInfoResolver, TitusFederationConfiguration federationConfiguration) {
        return new ChainCellRouter(Arrays.asList(
                SpecialInstanceTypeRouter.getGpuInstanceTypeRouter(cellInfoResolver, federationConfiguration),
                new ApplicationCellRouter(cellInfoResolver, federationConfiguration),
                new FallbackCellRouter(cellInfoResolver)
        ));
    }
}
