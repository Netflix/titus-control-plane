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

package com.netflix.titus.federation.endpoint.rest;

import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionHandlers;
import com.netflix.titus.runtime.endpoint.v3.rest.AutoScalingSpringResource;
import com.netflix.titus.runtime.endpoint.v3.rest.HealthSpringResource;
import com.netflix.titus.runtime.endpoint.v3.rest.JobManagementSpringResource;
import com.netflix.titus.runtime.endpoint.v3.rest.LoadBalancerSpringResource;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        TitusExceptionHandlers.class,

        HealthSpringResource.class,
        FederationSchedulerSpringResource.class,
        JobManagementSpringResource.class,
        AutoScalingSpringResource.class,
        LoadBalancerSpringResource.class,
        FederationV2CapacityGroupSpringResource.class,
})
public class FederationRestComponent {
}
