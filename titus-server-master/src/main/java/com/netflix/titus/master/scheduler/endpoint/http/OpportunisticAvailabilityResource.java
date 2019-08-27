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

package com.netflix.titus.master.scheduler.endpoint.http;

import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;
import io.swagger.annotations.Api;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Titus Opportunistic Resources")
@Path("/api/diagnostic/opportunistic")
@Singleton
public class OpportunisticAvailabilityResource {
    private final OpportunisticCpuAvailabilityProvider provider;

    @Inject
    public OpportunisticAvailabilityResource(OpportunisticCpuAvailabilityProvider provider) {
        this.provider = provider;
    }

    @GET
    @Path("/cpus")
    public Map<String, OpportunisticCpuAvailability> getAvailableOpportunisticCpusPerInstance() {
        return provider.getOpportunisticCpus();
    }
}
