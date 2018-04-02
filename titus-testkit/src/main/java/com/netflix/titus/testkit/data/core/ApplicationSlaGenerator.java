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

package com.netflix.titus.testkit.data.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.model.ApplicationSLA;

public class ApplicationSlaGenerator {

    private final Map<ApplicationSlaSample, Integer> sampleIndexes = new HashMap<>();
    private final ApplicationSlaSample[] samples;
    private int pos;

    public ApplicationSlaGenerator(ApplicationSlaSample... samples) {
        this.samples = samples;
        for (ApplicationSlaSample sample : samples) {
            sampleIndexes.put(sample, 0);
        }
    }

    public ApplicationSLA next() {
        ApplicationSlaSample sample = samples[pos % samples.length];
        pos++;

        int idx = sampleIndexes.get(sample);
        sampleIndexes.put(sample, idx + 1);
        ApplicationSLA.Builder builder = sample.builder().withAppName(sample.name() + '_' + idx);

        return builder.build();
    }

    public List<ApplicationSLA> next(int count) {
        List<ApplicationSLA> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(next());
        }
        return batch;
    }
}
