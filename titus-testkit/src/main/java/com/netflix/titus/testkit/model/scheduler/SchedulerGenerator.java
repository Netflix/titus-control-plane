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

package com.netflix.titus.testkit.model.scheduler;

import java.util.UUID;

import com.netflix.titus.api.scheduler.model.Match;
import com.netflix.titus.api.scheduler.model.Must;
import com.netflix.titus.api.scheduler.model.Should;
import com.netflix.titus.api.scheduler.model.SystemSelector;
import com.netflix.titus.common.data.generator.DataGenerator;

import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static com.netflix.titus.common.data.generator.DataGenerator.rangeInt;

public final class SchedulerGenerator {

    private SchedulerGenerator() {
    }

    public static DataGenerator<SystemSelector> shouldSystemSelectors() {
        return DataGenerator.bindBuilder(SystemSelector::newBuilder)
                .bind(range(0), (builder, idx) -> {
                    builder.withId(UUID.randomUUID().toString())
                            .withDescription("Description#" + idx)
                            .withEnabled(true)
                            .withPriority(rangeInt(1, 1000).getValue())
                            .withReason("Reason#" + idx)
                            .withTimestamp(System.currentTimeMillis());

                }).bind(should(), SystemSelector.Builder::withShould)
                .map(SystemSelector.Builder::build);
    }

    public static DataGenerator<SystemSelector> mustSystemSelectors() {
        return DataGenerator.bindBuilder(SystemSelector::newBuilder)
                .bind(range(0), (builder, idx) -> {
                    builder.withId(UUID.randomUUID().toString())
                            .withDescription("Description#" + idx)
                            .withEnabled(true)
                            .withPriority(rangeInt(1, 1000).getValue())
                            .withReason("Reason#" + idx)
                            .withTimestamp(System.currentTimeMillis());

                }).bind(must(), SystemSelector.Builder::withMust)
                .map(SystemSelector.Builder::build);
    }

    public static DataGenerator<Should> should() {
        return DataGenerator.bindBuilder(Should::newBuilder)
                .bind(match(), Should.Builder::withOperator)
                .map(Should.Builder::build);
    }

    public static DataGenerator<Must> must() {
        return DataGenerator.bindBuilder(Must::newBuilder)
                .bind(match(), Must.Builder::withOperator)
                .map(Must.Builder::build);
    }

    public static DataGenerator<Match> match() {
        return DataGenerator.bindBuilder(Match::newBuilder)
                .bind(range(0), (builder, idx) -> {
                    builder.withSelectExpression("SelectExpression#" + idx)
                            .withMatchExpression("MatchExpression#" + idx);

                }).map(Match.Builder::build);
    }
}