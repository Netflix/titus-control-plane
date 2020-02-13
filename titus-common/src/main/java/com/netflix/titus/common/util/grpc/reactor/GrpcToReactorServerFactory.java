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

package com.netflix.titus.common.util.grpc.reactor;

import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;

public interface GrpcToReactorServerFactory {

    <REACT_SERVICE> ServerServiceDefinition apply(ServiceDescriptor serviceDefinition, REACT_SERVICE reactService);

    /**
     * This method is used when the {@link REACT_SERVICE} is a CGLIB proxy and additional class details need to be provided to the factory.
     * If the {@link REACT_SERVICE} is a CGLIB proxy class it does not retain generic type information from the proxy target class so a
     * detailed description of the target class is needed.
     */
    <REACT_SERVICE> ServerServiceDefinition apply(ServiceDescriptor serviceDefinition, REACT_SERVICE reactService, Class<REACT_SERVICE> reactorDetailedFallbackClass);
}
