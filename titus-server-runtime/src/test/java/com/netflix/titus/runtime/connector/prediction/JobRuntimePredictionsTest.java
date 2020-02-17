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

package com.netflix.titus.runtime.connector.prediction;

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobRuntimePredictionsTest {

    @Test
    public void testOneLineRepresentation() {
        SortedSet<JobRuntimePrediction> predictions = new TreeSet<>();
        predictions.add(new JobRuntimePrediction(0.95, 26.2));
        predictions.add(new JobRuntimePrediction(0.1, 10.0));
        predictions.add(new JobRuntimePrediction(0.2, 15.0));
        JobRuntimePredictions jobRuntimePredictions = new JobRuntimePredictions("1", "some-id", predictions);
        Optional<String> asString = jobRuntimePredictions.toSimpleString();
        assertThat(asString).isPresent();
        assertThat(asString.get()).isEqualTo("0.1=10.0;0.2=15.0;0.95=26.2");
    }

    @Test
    public void testOneLineRepresentationForEmptyPredictions() {
        JobRuntimePredictions predictions = new JobRuntimePredictions("1", "some-id", new TreeSet<>());
        assertThat(predictions.toSimpleString()).isEmpty();
    }

}
