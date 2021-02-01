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

package com.netflix.titus.common.util.rx;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import static com.netflix.titus.common.util.rx.ReactorExt.mapWithState;
import static org.assertj.core.api.Assertions.assertThat;

public class ReactorMapWithStateTransformerTest {

    @Test
    public void testStatePropagation() {
        List<String> all = Flux.just("a", "b")
                .transformDeferred(mapWithState("START", (next, state) -> Pair.of(state + " -> " + next, next)))
                .collectList().block();
        assertThat(all).contains("START -> a", "a -> b");
    }

    @Test
    public void testStatePropagationWithCleanup() {
        DirectProcessor<String> source = DirectProcessor.create();
        DirectProcessor<Function<List<String>, Pair<String, List<String>>>> cleanupActions = DirectProcessor.create();

        TitusRxSubscriber<String> testSubscriber = new TitusRxSubscriber<>();
        source.transformDeferred(mapWithState(
                new ArrayList<>(),
                (next, state) -> Pair.of(
                        String.join(",", state) + " + " + next,
                        CollectionsExt.copyAndAdd(state, next)
                ),
                cleanupActions
        )).subscribe(testSubscriber);

        source.onNext("a");
        assertThat(testSubscriber.takeNext()).isEqualTo(" + a");

        source.onNext("b");
        assertThat(testSubscriber.takeNext()).isEqualTo("a + b");

        cleanupActions.onNext(list -> Pair.of("removed " + list.get(0), list.subList(1, list.size())));
        assertThat(testSubscriber.takeNext()).isEqualTo("removed a");

        source.onNext("c");
        assertThat(testSubscriber.takeNext()).isEqualTo("b + c");
    }
}