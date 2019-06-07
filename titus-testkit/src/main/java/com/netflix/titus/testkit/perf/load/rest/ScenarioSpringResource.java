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

package com.netflix.titus.testkit.perf.load.rest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator;
import com.netflix.titus.testkit.perf.load.plan.catalog.AgentExecutableGeneratorCatalog;
import com.netflix.titus.testkit.perf.load.plan.catalog.JobExecutableGeneratorCatalog;
import com.netflix.titus.testkit.perf.load.report.MetricsCollector;
import com.netflix.titus.testkit.perf.load.rest.representation.ScenarioExecutionRepresentation;
import com.netflix.titus.testkit.perf.load.rest.representation.ScenarioRepresentation;
import com.netflix.titus.testkit.perf.load.rest.representation.StartScenarioRequest;
import com.netflix.titus.testkit.perf.load.runner.AgentTerminator;
import com.netflix.titus.testkit.perf.load.runner.JobTerminator;
import com.netflix.titus.testkit.perf.load.runner.Orchestrator;
import com.netflix.titus.testkit.perf.load.runner.ScenarioRunner;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/")
public class ScenarioSpringResource {

    private static final Map<String, ScenarioRepresentation> SCENARIOS = ImmutableMap.<String, ScenarioRepresentation>builder()
            .put("empty", new ScenarioRepresentation(
                    "empty",
                    "Job execution scenario that does not create any job"
            ))
            .put("mixedLoad", new ScenarioRepresentation(
                    "mixedLoad",
                    "Includes all system actions (job management, agent deployment and migration, etc)"
            ))
            .put("perfLoad", new ScenarioRepresentation(
                    "perfLoad",
                    "Load test scenario for performance testing (accepts 'scaleFactor' and 'churnFactor' parameters)"
            ))
            .put("perfLoad2", new ScenarioRepresentation(
                    "perfLoad2",
                    "Load test scenario for performance testing (accepts 'totalTaskCount' and 'churnRateSec' parameters)"
            ))
            .put("batchJobs", new ScenarioRepresentation(
                    "batchJobs",
                    "Run batch jobs"
            ))
            .put("evictions", new ScenarioRepresentation(
                    "evictions",
                    "Runs service jobs with periodic random task eviction"
            ))
            .put("longRunning", new ScenarioRepresentation(
                    "longRunning",
                    "Long running services on the critical tier, with a mixed size of job sizes"
            ))
            .build();

    private final Orchestrator orchestrator;
    private final AgentTerminator agentTerminator;
    private final JobTerminator jobTerminator;

    @Inject
    public ScenarioSpringResource(Orchestrator orchestrator,
                                  AgentTerminator agentTerminator,
                                  JobTerminator jobTerminator) {
        this.orchestrator = orchestrator;
        this.agentTerminator = agentTerminator;
        this.jobTerminator = jobTerminator;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/scenarios", produces = MediaType.APPLICATION_JSON)
    public List<ScenarioRepresentation> getScenarios() {
        return new ArrayList<>(SCENARIOS.values());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/scenarios/{name}", produces = MediaType.APPLICATION_JSON)
    public ScenarioRepresentation getScenario(@PathVariable("name") String name) {
        ScenarioRepresentation scenario = SCENARIOS.get(name);
        if (scenario == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return scenario;
    }

    @RequestMapping(method = RequestMethod.POST, path = "/executions", consumes = MediaType.APPLICATION_JSON)
    public Response startScenario(@RequestBody StartScenarioRequest request) throws URISyntaxException {
        String jobPlan = request.getJobPlan();
        JobExecutableGenerator jobExecutableGenerator;

        int scaleFactor = request.getScaleFactor() <= 0 ? 1 : (int) request.getScaleFactor();
        double churnFactor = request.getChurnFactor() <= 0 ? 1 : request.getChurnFactor();

        if (StringExt.isEmpty(jobPlan) || jobPlan.equals("empty")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.empty();
        } else if (jobPlan.equals("mixedLoad")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.mixedLoad(request.getScaleFactor());
        } else if (jobPlan.equals("perfLoad")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.perfLoad(request.getScaleFactor(), churnFactor);
        } else if (jobPlan.equals("perfLoad2")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.perfLoad(request.getTotalTaskCount(), request.getChurnRateSec());
        } else if (jobPlan.equals("batchJobs")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.batchJobs(request.getJobSize(), scaleFactor);
        } else if (jobPlan.equals("evictions")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.evictions(request.getJobSize(), scaleFactor);
        } else if (jobPlan.equals("longRunning")) {
            jobExecutableGenerator = JobExecutableGeneratorCatalog.longRunningServicesLoad("longRunning");
        } else {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        String agentPlan = request.getAgentPlan();
        List<ExecutionPlan> agentExecutionPlans;
        if (StringExt.isEmpty(agentPlan) || agentPlan.equals("empty")) {
            agentExecutionPlans = Collections.emptyList();
        } else if (agentPlan.equals("perfLoad") || agentPlan.equals("perfLoad2")) {
            agentExecutionPlans = AgentExecutableGeneratorCatalog.perfLoad(scaleFactor);
        } else if (agentPlan.equals("longRunning")) {
            agentExecutionPlans = AgentExecutableGeneratorCatalog.longRunningLoad();
        } else {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("jobSize", request.getJobSize());
        context.put("scaleFactor", request.getScaleFactor());

        ScenarioRunner runner = orchestrator.startScenario(jobExecutableGenerator, agentExecutionPlans, context);
        return Response.created(new URI((runner.getScenarioExecutionId()))).build();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/executions", produces = MediaType.APPLICATION_JSON)
    public List<ScenarioExecutionRepresentation> getScenarioExecutions() {
        return orchestrator.getScenarioRunners().values().stream()
                .map(ScenarioSpringResource::toRepresentation)
                .collect(Collectors.toList());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/executions/{id}", produces = MediaType.APPLICATION_JSON)
    public ScenarioExecutionRepresentation getScenarioExecutions(@PathVariable("id") String id) {
        ScenarioRunner runner = orchestrator.getScenarioRunners().get(id);
        if (runner == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return toRepresentation(runner);
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/executions/{id}")
    public Response stopScenarioExecution(@PathVariable("id") String id) {
        ScenarioRunner runner = orchestrator.getScenarioRunners().get(id);
        if (runner == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        orchestrator.stopScenarioExecution(id);
        return Response.noContent().build();
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/executions")
    public Response stopAllScenarios(@RequestParam(name = "orphaned", required = false) boolean orphaned) {
        orchestrator.getScenarioRunners().forEach((id, runner) -> orchestrator.stopScenarioExecution(id));
        if (orphaned) {
            jobTerminator.doClean();
        }
        return Response.noContent().build();
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/agents")
    public Response removeAllAgents() {
        agentTerminator.doClean();
        return Response.noContent().build();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/report/metrics", produces = MediaType.APPLICATION_JSON)
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
