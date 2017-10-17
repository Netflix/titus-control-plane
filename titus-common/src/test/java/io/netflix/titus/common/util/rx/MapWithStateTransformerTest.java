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

package io.netflix.titus.common.util.rx;


import java.util.List;

import io.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;
import rx.Observable;

import static io.netflix.titus.common.util.rx.ObservableExt.mapWithState;
import static org.assertj.core.api.Assertions.assertThat;

public class MapWithStateTransformerTest {

    @Test
    public void testStatePropagation() throws Exception {
        List<String> all = Observable.just("a", "b")
                .compose(mapWithState("START", (next, state) -> Pair.of(state + " -> " + next, next)))
                .toList().toBlocking().first();
        assertThat(all).contains("START -> a", "a -> b");
    }
}