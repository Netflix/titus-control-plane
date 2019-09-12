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

package com.netflix.titus.common.util.grpc.reactor;

import com.netflix.titus.testing.SampleGrpcService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface SampleServiceReactorClient {

    Mono<SampleGrpcService.SampleContainer> getOneValue();

    Mono<SampleGrpcService.SampleContainer> getOneValue(SampleContext context);

    Mono<Void> setOneValue(SampleGrpcService.SampleContainer value);

    Mono<Void> setOneValue(SampleGrpcService.SampleContainer value, SampleContext context);

    Flux<SampleGrpcService.SampleContainer> stream();

    Flux<SampleGrpcService.SampleContainer> stream(SampleContext context);
}
