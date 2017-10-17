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

package io.netflix.titus.testkit.perf.load;

import java.util.concurrent.CountDownLatch;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.testkit.perf.load.catalog.ExecutionScenarioCatalog;
import io.netflix.titus.testkit.perf.load.job.ActiveJobsMonitor;
import io.netflix.titus.testkit.perf.load.report.MetricsCollector;
import io.netflix.titus.testkit.perf.load.report.TextReporter;
import io.netflix.titus.testkit.perf.load.runner.ExecutionScenarioRunner;
import io.netflix.titus.testkit.perf.load.runner.Terminator;
import rx.schedulers.Schedulers;

@Singleton
public class Orchestrator {

    private final ExecutionScenarioRunner scenarioRunner;
    private final MetricsCollector metricsCollector;
    private final TextReporter textReporter;

    private final CountDownLatch doneLatch = new CountDownLatch(1);

    @Inject
    public Orchestrator(LoadConfiguration configuration,
                        ActiveJobsMonitor activeJobsMonitor,
                        Terminator terminator,
                        ExecutionContext context) {
        if (configuration.isClean()) {
            terminator.doClean();
        }

        this.scenarioRunner = new ExecutionScenarioRunner(
//                ExecutionScenarioCatalog.oneAutoScalingService(configuration.getScaleFactor()),
                ExecutionScenarioCatalog.oneScalingServiceWihTerminateAndShrink(configuration.getScaleFactor()),
//                ExecutionScenarioCatalog.oneBatchJob(configuration.getScaleFactor()),
//                ExecutionScenarioCatalog.mixedLoad(configuration.getScaleFactor()),
                activeJobsMonitor,
                context,
                Schedulers.computation()
        );
        this.metricsCollector = new MetricsCollector();
        metricsCollector.watch(scenarioRunner.start().doOnUnsubscribe(doneLatch::countDown));
        this.textReporter = new TextReporter(metricsCollector, Schedulers.computation());
        textReporter.start();
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public void awaitTermination() {
        try {
            doneLatch.await();
        } catch (InterruptedException ignore) {
        }
    }
}
