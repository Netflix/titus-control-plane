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

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.testkit.perf.load.Orchestrator;
import com.netflix.titus.testkit.perf.load.report.MetricsCollector;

@Path("/report")
@Singleton
public class ReportResource {

    private final MetricsCollector metricsCollector;

    @Inject
    public ReportResource(Orchestrator orchestrator) {
        this.metricsCollector = orchestrator.getMetricsCollector();
    }

    @GET
    public Map<String, Object> getReport() {
        Map<String, String> runningTaskStates = new HashMap<>();
        metricsCollector.getActiveTaskStateCounters().forEach((k, v) -> runningTaskStates.put(k.toString(), v.toString()));

        return ImmutableMap.<String, Object>builder()
                .put("totals",
                        ImmutableMap.builder()
                                .put("allJobs", metricsCollector.getTotalSubmittedJobs())
                                .put("allInconsistencies", metricsCollector.getTotalInconsistencies())
                                .build()
                )
                .put("active",
                        ImmutableMap.builder()
                                .put("activeJobs", metricsCollector.getActiveJobs())
                                .put("pendingInconsistencies", metricsCollector.getPendingInconsistencies())
                                .put("runningTaskStates", runningTaskStates)
                                .build()
                )
                .build();
    }
}
