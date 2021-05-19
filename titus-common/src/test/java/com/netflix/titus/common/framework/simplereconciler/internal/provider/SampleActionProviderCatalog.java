/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal.provider;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProvider;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy;
import reactor.core.publisher.Mono;

public class SampleActionProviderCatalog {

    private static final Function<String, List<Mono<Function<String, String>>>> NO_OP = d -> Collections.emptyList();

    public static ReconcilerActionProvider<String> newExternalProvider(String name) {
        return new ReconcilerActionProvider<>(
                ReconcilerActionProviderPolicy.getDefaultExternalPolicy().toBuilder().withName(name).build(),
                true,
                NO_OP
        );
    }

    public static ReconcilerActionProvider<String> newInternalProvider(String name, int priority, Duration executionInterval, Duration minExecutionInterval) {
        return new ReconcilerActionProvider<>(
                ReconcilerActionProviderPolicy.getDefaultInternalPolicy().toBuilder()
                        .withName(name)
                        .withPriority(priority)
                        .withExecutionInterval(executionInterval)
                        .withMinimumExecutionInterval(minExecutionInterval)
                        .build(),
                false,
                new SampleProviderActionFunction("#" + name)
        );
    }

    public static ReconcilerActionProvider<String> newInternalProvider(String name, int priority) {
        return newInternalProvider(name, priority, Duration.ZERO, Duration.ZERO);
    }

    public static class SampleProviderActionFunction implements Function<String, List<Mono<Function<String, String>>>> {

        private final String suffixToAdd;
        private boolean emitEnabled;

        public SampleProviderActionFunction(String suffixToAdd) {
            this.suffixToAdd = suffixToAdd;
        }

        public void enableEmit(boolean emitEnabled) {
            this.emitEnabled = emitEnabled;
        }

        @Override
        public List<Mono<Function<String, String>>> apply(String data) {
            if (emitEnabled) {
                return Collections.singletonList(Mono.just(d -> d + suffixToAdd));
            }
            return Collections.emptyList();
        }
    }
}
