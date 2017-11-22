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

package io.netflix.titus.gateway.endpoint.v3.grpc;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;

/**
 */
@Configuration(prefix = "titusGateway.endpoint.grpc")
public interface GrpcEndpointConfiguration {

    @PropertyName(name = "port")
    @DefaultValue("7104")
    int getPort();

    @PropertyName(name = "loadbalancer.enabled")
    @DefaultValue("true")
    boolean getLoadBalancerGrpcEnabled();

    /**
     * Graceful shutdown time for GRPC server. If zero, shutdown happens immediately, and all client connections are
     * terminated abruptly.
     */
    @DefaultValue("30000")
    long getShutdownTimeoutMs();
}
