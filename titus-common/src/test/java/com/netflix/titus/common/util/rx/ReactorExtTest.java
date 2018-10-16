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

package com.netflix.titus.common.util.rx;

import java.util.Optional;

import org.junit.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorExtTest {

    @Test
    public void testEmitValue() {
        Optional<Throwable> error = ReactorExt.emitError(Mono.just("Hello")).block();
        assertThat(error).isEmpty();
    }

    @Test
    public void testEmitError() {
        Optional<Throwable> error = ReactorExt.emitError(Mono.error(new RuntimeException("SimulatedError"))).single().block();
        assertThat(error).isPresent();
        assertThat(error.get()).isInstanceOf(RuntimeException.class);
    }
}