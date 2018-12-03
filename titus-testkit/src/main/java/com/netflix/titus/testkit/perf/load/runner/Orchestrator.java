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

package com.netflix.titus.testkit.perf.load.runner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import com.netflix.titus.testkit.perf.load.report.MetricsCollector;
import com.netflix.titus.testkit.perf.load.report.TextReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;

@Singleton
public class Orchestrator {

    private static final Logger logger = LoggerFactory.getLogger(Orchestrator.class);

    private final ExecutionContext context;
    private final MetricsCollector metricsCollector;
    private final TextReporter textReporter;

    private final AtomicInteger nextSequenceId = new AtomicInteger();

    private final ConcurrentMap<String, ScenarioRunner> scenarioRunners = new ConcurrentHashMap<>();

    @Inject
    public Orchestrator(ExecutionContext context) {
        this.context = context;
        this.metricsCollector = new MetricsCollector();
        metricsCollector.watch(context);
        this.textReporter = new TextReporter(metricsCollector, Schedulers.computation());
        textReporter.start();
    }

    @PreDestroy
    public void shutdown() {
        textReporter.stop();
    }

    public Map<String, ScenarioRunner> getScenarioRunners() {
        return scenarioRunners;
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public ScenarioRunner startScenario(ExecutionScenario executionScenario, Map<String, Object> requestContext) {
        logger.info("Starting new scenario: " + executionScenario);

        String id = this.context.getSessionId() + '$' + nextSequenceId.getAndIncrement();
        ScenarioRunner scenarioRunner = new ScenarioRunner(id, requestContext, executionScenario, context);
        scenarioRunners.put(id, scenarioRunner);
        return scenarioRunner;
    }

    public void stopScenarioExecution(String id) {
        ScenarioRunner runner = scenarioRunners.remove(id);
        if (runner == null) {
            throw new IllegalArgumentException("Scenario execution not found: " + id);
        }

        logger.info("Stopping scenario: " + id);
        runner.shutdown();
    }
}
