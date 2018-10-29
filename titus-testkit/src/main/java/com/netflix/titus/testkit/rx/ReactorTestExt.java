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

package com.netflix.titus.testkit.rx;

import java.util.function.Supplier;

import com.netflix.titus.testkit.rx.internal.DirectedProcessor;
import reactor.core.publisher.FluxProcessor;

public final class ReactorTestExt {

    private ReactorTestExt() {
    }

    public static <T> DirectedProcessor<T> newDirectedProcessor(Supplier<FluxProcessor<T, T>> processorSupplier) {
        return new DirectedProcessor<>(processorSupplier);
    }
}
