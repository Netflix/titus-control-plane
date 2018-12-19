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

package com.netflix.titus.supplementary.relocation.endpoint.rest;

import com.netflix.titus.runtime.endpoint.metadata.SimpleGrpcCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleHttpCallMetadataResolver;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;

@Configuration
public class TaskRelocationRestComponent {

    public static final String HTTP_RESOLVER = "http";
    public static final String GRPC_RESOLVER = "grpc";

    @Bean(name = HTTP_RESOLVER)
    public SimpleHttpCallMetadataResolver getSimpleHttpCallMetadataResolver() {
        return new SimpleHttpCallMetadataResolver();
    }

    @Bean(name = GRPC_RESOLVER)
    public SimpleGrpcCallMetadataResolver getSimpleGrpcCallMetadataResolver() {
        return new SimpleGrpcCallMetadataResolver();
    }

    @Bean
    public FilterRegistrationBean<SimpleHttpCallMetadataResolver.CallMetadataInterceptorFilter> CallMetadataInterceptorFilter(SimpleHttpCallMetadataResolver resolver) {
        FilterRegistrationBean<SimpleHttpCallMetadataResolver.CallMetadataInterceptorFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new SimpleHttpCallMetadataResolver.CallMetadataInterceptorFilter(resolver));
        registrationBean.addUrlPatterns("/*");
        return registrationBean;
    }

    @Bean
    public ProtobufHttpMessageConverter protobufHttpMessageConverter() {
        return new ProtobufHttpMessageConverter();
    }
}
