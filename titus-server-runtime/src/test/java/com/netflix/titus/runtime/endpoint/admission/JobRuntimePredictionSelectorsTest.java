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

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableMap;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.config.MapConfig;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.util.closeable.CloseableReference;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePredictions;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.runtime.endpoint.admission.JobRuntimePredictionSelectors.aboveThreshold;
import static com.netflix.titus.runtime.endpoint.admission.JobRuntimePredictionSelectors.aboveThresholds;
import static com.netflix.titus.runtime.endpoint.admission.JobRuntimePredictionSelectors.reloadedOnConfigurationUpdate;
import static org.assertj.core.api.Assertions.assertThat;

public class JobRuntimePredictionSelectorsTest {

    private static final JobDescriptor JOB_DESCRIPTOR = JobDescriptorGenerator.oneTaskBatchJobDescriptor();

    private static final JobRuntimePredictions JOB_PREDICTIONS = new JobRuntimePredictions("v1", "m1",
            new TreeSet<>(Arrays.asList(
                    new JobRuntimePrediction(0.05, 5),
                    new JobRuntimePrediction(0.25, 10),
                    new JobRuntimePrediction(0.95, 20)
            )));

    private static final JobRuntimePredictions BAD_CONFIDENCE_LEVELS = new JobRuntimePredictions("v1", "m1",
            new TreeSet<>(Arrays.asList(
                    new JobRuntimePrediction(0.1, 5),
                    new JobRuntimePrediction(0.5, 10),
                    new JobRuntimePrediction(0.9, 20)
            )));

    private static final Map<String, String> METADATA = Collections.singletonMap("key", "value");

    @Test
    public void testAboveThreshold() {
        JobRuntimePredictionSelector selector = aboveThreshold(60, 10, METADATA);
        checkSelection(selector.apply(JOB_DESCRIPTOR, JOB_PREDICTIONS));
    }

    @Test
    public void testAboveThresholdWithWrongConfidenceLevels() {
        JobRuntimePredictionSelector selector = aboveThreshold(60, 10, METADATA);
        Optional<JobRuntimePredictionSelection> selectionOpt = selector.apply(JOB_DESCRIPTOR, BAD_CONFIDENCE_LEVELS);
        assertThat(selectionOpt).isEmpty();
    }

    @Test
    public void testAboveThresholdWithLowRuntimeThreshold() {
        JobRuntimePredictionSelector selector = aboveThreshold(10, 1, METADATA);
        Optional<JobRuntimePredictionSelection> selectionOpt = selector.apply(JOB_DESCRIPTOR, JOB_PREDICTIONS);
        assertThat(selectionOpt).isEmpty();
    }

    @Test
    public void testAboveThresholdFromConfig() {
        ImmutableMap<String, String> config = ImmutableMap.<String, String>builder()
                .put("runtimeThresholdInSeconds", "60")
                .put("sigmaThreshold", "10")
                .build();
        JobRuntimePredictionSelector selector = aboveThreshold(new MapConfig(config), METADATA);
        checkSelection(selector.apply(JOB_DESCRIPTOR, JOB_PREDICTIONS));
    }

    @Test
    public void testAboveThresholds() {
        ImmutableMap<String, String> config = ImmutableMap.<String, String>builder()
                .put("root.cellA.runtimeThresholdInSeconds", "60")
                .put("root.cellA.sigmaThreshold", "10")
                .put("root.cellB.runtimeThresholdInSeconds", "6")
                .put("root.cellB.sigmaThreshold", "1")
                .build();
        Map<String, JobRuntimePredictionSelector> selectors = aboveThresholds(new MapConfig(config).getPrefixedView("root"), METADATA);
        checkSelection(selectors.get("cellA").apply(JOB_DESCRIPTOR, JOB_PREDICTIONS));
        assertThat(selectors.get("cellB").apply(JOB_DESCRIPTOR, JOB_PREDICTIONS)).isEmpty();
    }

    @Test
    public void testAbTestRandom() {
        JobRuntimePredictionSelector selector = JobRuntimePredictionSelectors.abTestRandom(
                ImmutableMap.<String, JobRuntimePredictionSelector>builder()
                        .put("cellA", JobRuntimePredictionSelectors.best())
                        .put("cellB", JobRuntimePredictionSelectors.best())
                        .build()
        );
        Set<String> observed = new HashSet<>();
        await().until(() -> {
            selector.apply(JOB_DESCRIPTOR, JOB_PREDICTIONS).ifPresent(s ->
                    observed.add(s.getMetadata().get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AB_TEST))
            );
            return observed.size() == 2;
        });
    }

    @Test
    public void testReloadedOnConfigurationUpdate() {
        SettableConfig config = new DefaultSettableConfig();
        config.setProperty("root.runtimeThresholdInSeconds", "60");
        config.setProperty("root.sigmaThreshold", "10");

        CloseableReference<JobRuntimePredictionSelector> selectorRef = reloadedOnConfigurationUpdate(
                config.getPrefixedView("root"),
                configUpdate -> aboveThreshold(configUpdate, METADATA)
        );
        JobRuntimePredictionSelector selector = selectorRef.get();
        checkSelection(selector.apply(JOB_DESCRIPTOR, JOB_PREDICTIONS));

        // Now change it to fail
        config.setProperty("root.runtimeThresholdInSeconds", "6");
        assertThat(selector.apply(JOB_DESCRIPTOR, JOB_PREDICTIONS)).isEmpty();

        selectorRef.close();
    }

    private void checkSelection(Optional<JobRuntimePredictionSelection> selectionOpt) {
        assertThat(selectionOpt).isNotEmpty();
        assertThat(selectionOpt.get().getPrediction().getConfidence()).isEqualTo(0.95);
        assertThat(selectionOpt.get().getMetadata()).containsAllEntriesOf(METADATA);
    }
}