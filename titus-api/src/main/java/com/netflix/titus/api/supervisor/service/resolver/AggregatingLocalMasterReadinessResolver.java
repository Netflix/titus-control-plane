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

package com.netflix.titus.api.supervisor.service.resolver;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import reactor.core.publisher.Flux;

public class AggregatingLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    private final Flux<ReadinessStatus> stream;
    private final Clock clock;

    public AggregatingLocalMasterReadinessResolver(List<LocalMasterReadinessResolver> delegates, TitusRuntime titusRuntime) {
        this.clock = titusRuntime.getClock();
        this.stream = Flux.combineLatest(
                delegates.stream().map(LocalMasterReadinessResolver::observeLocalMasterReadinessUpdates).collect(Collectors.toList()),
                this::combine
        );
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return stream;
    }

    private ReadinessStatus combine(Object[] values) {
        for (Object value : values) {
            ReadinessStatus status = (ReadinessStatus) value;
            if (status.getState() != ReadinessState.Enabled) {
                return status;
            }
        }
        return ReadinessStatus.newBuilder()
                .withState(ReadinessState.Enabled)
                .withMessage("All master readiness components report status Enabled")
                .withTimestamp(clock.wallTime())
                .build();
    }
}
