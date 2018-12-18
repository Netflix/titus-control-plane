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

package com.netflix.titus.supplementary.relocation.endpoint;

import javax.inject.Named;

import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CompositeCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleGrpcCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleHttpCallMetadataResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import static com.netflix.titus.supplementary.relocation.endpoint.rest.TaskRelocationRestComponent.HTTP_RESOLVER;
import static java.util.Arrays.asList;

@Configuration
public class RelocationEndpointComponent {

    @Bean
    @Primary
    public CallMetadataResolver getCallMetadataResolver(@Named(HTTP_RESOLVER) SimpleHttpCallMetadataResolver simpleHttpCallMetadataResolver) {
        return new CompositeCallMetadataResolver(
                asList(new SimpleGrpcCallMetadataResolver(), simpleHttpCallMetadataResolver)
        );
    }
}
