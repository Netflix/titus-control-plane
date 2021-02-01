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

package com.netflix.titus.common.util.rx;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.SignalType;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorRetriersTest {

    @Test
    public void testRectorPredicateRetryer() {
        AtomicBoolean retryRef = new AtomicBoolean();
        Iterator<Signal<String>> it = Flux.fromArray(new String[]{"a", "b", "c"})
                .flatMap(value -> {
                    if (value.equals("b")) {
                        return Mono.error(new RuntimeException("simulated error"));
                    }
                    return Mono.just(value);
                })
                .retryWhen(ReactorRetriers.rectorPredicateRetryer(error -> !retryRef.getAndSet(true)))
                .materialize()
                .toIterable()
                .iterator();
        assertThat(it.next().get()).isEqualTo("a");
        assertThat(it.next().get()).isEqualTo("a");
        Signal<String> third = it.next();
        assertThat(third.getType()).isEqualTo(SignalType.ON_ERROR);
        assertThat(third.getThrowable()).isInstanceOf(RuntimeException.class);
    }
}