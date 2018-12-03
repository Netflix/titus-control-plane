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

package com.netflix.titus.testkit.perf.load.rest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.testkit.perf.load.catalog.ExecutionScenarioCatalog;
import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import com.netflix.titus.testkit.perf.load.report.MetricsCollector;
import com.netflix.titus.testkit.perf.load.rest.representation.ScenarioExecutionRepresentation;
import com.netflix.titus.testkit.perf.load.rest.representation.ScenarioRepresentation;
import com.netflix.titus.testkit.perf.load.rest.representation.StartScenarioRequest;
import com.netflix.titus.testkit.perf.load.runner.Orchestrator;
import com.netflix.titus.testkit.perf.load.runner.ScenarioRunner;
import com.netflix.titus.testkit.perf.load.runner.Terminator;

@Path("/")
@Singleton
public class ScenarioResource {

    private static final Map<String, ScenarioRepresentation> SCENARIOS = ImmutableMap.<String, ScenarioRepresentation>builder()
            .put("mixedLoad", new ScenarioRepresentation(
                    "mixedLoad",
                    "Includes all system actions (job management, agent deployment and migration, etc)"
            ))
            .put("batchJobs", new ScenarioRepresentation(
                    "batchJobs",
                    "Run batch jobs"
            ))
            .put("evictions", new ScenarioRepresentation(
                    "evictions",
                    "Runs service jobs with periodic random task eviction"
            ))
            .build();

    private final Orchestrator orchestrator;
    private final Terminator terminator;

    @Inject
    public ScenarioResource(Orchestrator orchestrator,
                            Terminator terminator) {
        this.orchestrator = orchestrator;
        this.terminator = terminator;
    }

    @GET
    @Path("/scenarios")
    public List<ScenarioRepresentation> getScenarios() {
        return new ArrayList<>(SCENARIOS.values());
    }

    @GET
    @Path("/scenarios/{name}")
    public ScenarioRepresentation getScenario(@QueryParam("name") String name) {
        ScenarioRepresentation scenario = SCENARIOS.get(name);
        if (scenario == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return scenario;
    }

    @POST
    @Path("/executions")
    public Response startScenario(StartScenarioRequest request) throws URISyntaxException {
        String name = request.getName();
        ExecutionScenario executionScenario;

        if (name.equals("mixedLoad")) {
            executionScenario = ExecutionScenarioCatalog.mixedLoad(request.getScaleFactor());
        } else if (name.equals("batchJobs")) {
            executionScenario = ExecutionScenarioCatalog.batchJobs(request.getJobSize(), request.getScaleFactor());
        } else if (name.equals("evictions")) {
            executionScenario = ExecutionScenarioCatalog.evictions(request.getJobSize(), request.getScaleFactor());
        } else {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("jobSize", request.getJobSize());
        context.put("scaleFactor", request.getScaleFactor());

        ScenarioRunner runner = orchestrator.startScenario(executionScenario, context);
        return Response.created(new URI((runner.getScenarioExecutionId()))).build();
    }

    @GET
    @Path("/executions")
    public List<ScenarioExecutionRepresentation> getScenarioExecutions() {
        return orchestrator.getScenarioRunners().values().stream()
                .map(ScenarioResource::toRepresentation)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/executions/{id}")
    public ScenarioExecutionRepresentation getScenarioExecutions(@PathParam("id") String id) {
        ScenarioRunner runner = orchestrator.getScenarioRunners().get(id);
        if (runner == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return toRepresentation(runner);
    }

    @DELETE
    @Path("/executions/{id}")
    public Response stopScenarioExecution(@PathParam("id") String id) {
        ScenarioRunner runner = orchestrator.getScenarioRunners().get(id);
        if (runner == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        orchestrator.stopScenarioExecution(id);
        return Response.noContent().build();
    }

    @DELETE
    @Path("/executions")
    public Response stopAllScenarios(@QueryParam("orphaned") boolean orphaned) {
        orchestrator.getScenarioRunners().forEach((id, runner) -> orchestrator.stopScenarioExecution(id));
        if (orphaned) {
            terminator.doClean();
        }
        return Response.noContent().build();
    }

    @GET
    @Path("/report/metrics")
    public Map<String, Object> getMetrics() {
        MetricsCollector metrics = orchestrator.getMetricsCollector();

        Map<String, Object> metricsMap = new HashMap<>();
        metricsMap.put("activeJobs", metrics.getActiveJobs());
        metricsMap.put("activeTaskStateCounters", metrics.getActiveTaskStateCounters());
        metricsMap.put("pendingInconsistencies", metrics.getPendingInconsistencies());
        metricsMap.put("totalInconsistencies", metrics.getTotalInconsistencies());
        metricsMap.put("totalSubmittedJobs", metrics.getTotalSubmittedJobs());
        metricsMap.put("totalJobStatusCounters", metrics.getTotalJobStatusCounters());
        metricsMap.put("totalTaskStateCounters", metrics.getTotalTaskStateCounters());

        return metricsMap;
    }

    private static ScenarioExecutionRepresentation toRepresentation(ScenarioRunner runner) {
        return new ScenarioExecutionRepresentation(
                runner.getScenarioExecutionId(),
                runner.getRequestContext()
        );
    }
}
