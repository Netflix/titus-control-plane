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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.netflix.archaius.api.Config;
import com.netflix.archaius.api.ConfigListener;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.PropertiesExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.closeable.CloseableReference;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SELECTOR_INFO;

public final class JobRuntimePredictionSelectors {

    private static final Logger logger = LoggerFactory.getLogger(JobRuntimePredictionSelectors.class);

    private static final JobRuntimePredictionSelector NEVER_SELECTOR = (jobDescriptor, predictions) -> Optional.empty();

    private JobRuntimePredictionSelectors() {
    }

    public static JobRuntimePredictionSelector never() {
        return NEVER_SELECTOR;
    }

    /**
     * Always select the highest confidence prediction.
     */
    public static JobRuntimePredictionSelector best() {
        return best(Collections.emptyMap());
    }

    /**
     * Always select the highest confidence prediction, with additional metadata.
     */
    public static JobRuntimePredictionSelector best(Map<String, String> selectionMetadata) {
        Map<String, String> allMetadata = appendSelectorInfoIfMissing(
                selectionMetadata,
                () -> "best, createdAt: " + DateTimeExt.toLocalDateTimeString(System.currentTimeMillis())
        );
        return (jobDescriptor, predictions) -> CollectionsExt.isNullOrEmpty(predictions.getPredictions())
                ? Optional.empty()
                : Optional.of(JobRuntimePredictionSelection.newBuilder().withPrediction(predictions.getPredictions().last()).withMetadata(allMetadata).build());
    }

    /**
     * Select prediction with the highest confidence if it is below `runtimeThresholdInSeconds` and if the estimated
     * standard deviation of the predicted distribution is below `sigmaThreshold`
     */
    public static JobRuntimePredictionSelector aboveThreshold(double runtimeThresholdInSeconds,
                                                              double sigmaThreshold,
                                                              double quantile,
                                                              Map<String, String> selectionMetadata) {
        Map<String, String> allMetadata = appendSelectorInfoIfMissing(
                selectionMetadata,
                () -> String.format("aboveThreshold{runtimeThresholdInSeconds=%s, sigmaThreshold=%s}, createdAt: %s",
                        runtimeThresholdInSeconds, sigmaThreshold,
                        DateTimeExt.toLocalDateTimeString(System.currentTimeMillis())
                )
        );

        return (jobDescriptor, predictions) -> {
            SortedSet<JobRuntimePrediction> predictionsSet = predictions.getPredictions();
            if (CollectionsExt.isNullOrEmpty(predictionsSet) || predictionsSet.size() < 2) {
                return Optional.empty();
            }

            JobRuntimePrediction first = predictionsSet.first();
            if (!JobRuntimePredictionUtil.expectedLowest(first)) {
                return Optional.empty();
            }
            JobRuntimePrediction requested = JobRuntimePredictionUtil.findRequested(predictionsSet, quantile).orElse(null);
            if (requested == null || requested.getRuntimeInSeconds() > runtimeThresholdInSeconds) {
                return Optional.empty();
            }

            double sigma = (requested.getRuntimeInSeconds() - first.getRuntimeInSeconds()) / JobRuntimePredictionUtil.NORM_SIGMA;
            if (sigma > sigmaThreshold) {
                return Optional.empty();
            }
            return Optional.of(JobRuntimePredictionSelection.newBuilder()
                    .withPrediction(requested)
                    .withMetadata(allMetadata)
                    .build()
            );
        };
    }

    /**
     * See {@link #aboveThreshold(double, double, double, Map)}.
     */
    public static JobRuntimePredictionSelector aboveThreshold(Config config, Map<String, String> selectionMetadata) {
        double runtimeThresholdInSeconds = config.getDouble("runtimeThresholdInSeconds", Double.MAX_VALUE);
        double sigmaThreshold = config.getDouble("sigmaThreshold", Double.MAX_VALUE);
        double quantile = config.getDouble("quantile", JobRuntimePredictionUtil.HIGH_QUANTILE);
        return aboveThreshold(runtimeThresholdInSeconds, sigmaThreshold, quantile, selectionMetadata);
    }

    /**
     * Create a map threshold selectors. The first segment in the key name is a selector name.
     */
    public static Map<String, JobRuntimePredictionSelector> aboveThresholds(Config config, Map<String, String> selectionMetadata) {
        String[] keys = Iterators.toArray(config.getKeys(), String.class);
        Set<String> cellNames = PropertiesExt.getRootNames(Arrays.asList(keys), 1);

        Map<String, Config> subConfigs = new HashMap<>();
        cellNames.forEach(cell -> subConfigs.put(cell, config.getPrefixedView(cell)));

        Map<String, String> allMetadata = appendSelectorInfoIfMissing(
                selectionMetadata,
                () -> String.format("aboveThresholds[%s], createdAt: %s",
                        new TreeSet<>(subConfigs.keySet()).stream().map(k -> k + '=' + Archaius2Ext.toString(subConfigs.get(k))).collect(Collectors.joining(", ")) + ']',
                        DateTimeExt.toLocalDateTimeString(System.currentTimeMillis())
                )
        );

        return CollectionsExt.mapValues(subConfigs, cell -> aboveThreshold(cell, allMetadata), HashMap::new);
    }

    /**
     * A helper selector which listens to configuration changes, and reloads downstream selectors when that happens.
     */
    public static CloseableReference<JobRuntimePredictionSelector> reloadedOnConfigurationUpdate(Config config, Function<Config, JobRuntimePredictionSelector> factory) {
        AtomicReference<JobRuntimePredictionSelector> last = new AtomicReference<>(factory.apply(config));

        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigAdded(Config config) {
                tryReload(config);
            }

            @Override
            public void onConfigRemoved(Config config) {
                tryReload(config);
            }

            @Override
            public void onConfigUpdated(Config config) {
                tryReload(config);
            }

            @Override
            public void onError(Throwable error, Config config) {
                tryReload(config);
            }

            private void tryReload(Config updated) {
                try {
                    // Use the past config, in case it is prefixed view
                    last.set(factory.apply(config));
                } catch (Exception e) {
                    logger.info("Failed to reload job runtime prediction selectors", e);
                }
            }
        };
        config.addListener(listener);

        JobRuntimePredictionSelector wrapper = (jobDescriptor, predictions) -> last.get().apply(jobDescriptor, predictions);

        return CloseableReference.<JobRuntimePredictionSelector>newBuilder()
                .withResource(wrapper)
                .withCloseAction(() -> config.removeListener(listener))
                .withSwallowException(true)
                .withSerialize(true)
                .build();
    }

    /**
     * AB test choosing randomly from the provided collection of selectors. At each invocation, a one
     * downstream selector is chosen, and the result is tagged with its id.
     */
    public static JobRuntimePredictionSelector abTestRandom(Map<String, JobRuntimePredictionSelector> selectors) {
        if (CollectionsExt.isNullOrEmpty(selectors)) {
            return never();
        }
        List<Map.Entry<String, JobRuntimePredictionSelector>> entries = new ArrayList<>(selectors.entrySet());
        Random random = new Random(System.nanoTime());

        return (jobDescriptor, predictions) -> {

            Map.Entry<String, JobRuntimePredictionSelector> entry = entries.get(random.nextInt(selectors.size()));
            String id = entry.getKey();
            JobRuntimePredictionSelector selector = entry.getValue();

            return selector.apply(jobDescriptor, predictions).map(selection -> {
                Map<String, String> metadata = CollectionsExt.copyAndAdd(selection.getMetadata(), JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AB_TEST, id);
                return selection.toBuilder().withMetadata(metadata).build();
            });
        };
    }

    private static Map<String, String> appendSelectorInfoIfMissing(Map<String, String> selectionMetadata, Supplier<String> infoSupplier) {
        return selectionMetadata.containsKey(JOB_ATTRIBUTES_RUNTIME_PREDICTION_SELECTOR_INFO)
                ? selectionMetadata
                : CollectionsExt.copyAndAdd(selectionMetadata, JOB_ATTRIBUTES_RUNTIME_PREDICTION_SELECTOR_INFO, infoSupplier.get());
    }
}
