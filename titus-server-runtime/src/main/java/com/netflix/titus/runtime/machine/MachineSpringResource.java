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

package com.netflix.titus.runtime.machine;

import java.time.Duration;
import javax.inject.Inject;

import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.v4.Id;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineQueryResult;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import com.netflix.titus.runtime.connector.machine.ReactorMachineServiceStub;
import com.netflix.titus.runtime.endpoint.v3.rest.RestUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "Machine resources")
@RestController
@RequestMapping(path = "/api/v4/machines", produces = "application/json")
public class MachineSpringResource {

    // We depend on client timeouts, which are dynamically configurable. This is just reasonable upper bound if
    // they do not trigger.
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final ReactorMachineServiceStub machineServiceStub;

    @Inject
    public MachineSpringResource(ReactorMachineServiceStub machineServiceStub) {
        this.machineServiceStub = machineServiceStub;
    }

    @ApiOperation("Get all machines")
    @GetMapping
    public MachineQueryResult getMachines(@RequestParam MultiValueMap<String, String> queryParameters) {
        QueryRequest.Builder queryBuilder = QueryRequest.newBuilder();
        Page page = RestUtil.createPage(queryParameters);

        queryBuilder.setPage(page);
        queryBuilder.putAllFilteringCriteria(RestUtil.getFilteringCriteria(queryParameters));
        queryBuilder.addAllFields(RestUtil.getFieldsParameter(queryParameters));

        return machineServiceStub.getMachines(queryBuilder.build()).block(TIMEOUT);
    }

    @GetMapping(path = "/{machineId}")
    @ApiOperation("Get single machine")
    public Machine getMachine(@PathVariable("machineId") String machineId) {
        return machineServiceStub.getMachine(Id.newBuilder().setId(machineId).build()).block(TIMEOUT);
    }
}
