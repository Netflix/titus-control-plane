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

import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import reactor.core.publisher.Flux;

public class AlwaysEnabledLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    private static final ReadinessStatus ALWAYS_READY = ReadinessStatus.newBuilder()
            .withState(ReadinessState.Enabled)
            .withMessage("Always enabled")
            .build();

    private static final AlwaysEnabledLocalMasterReadinessResolver INSTANCE = new AlwaysEnabledLocalMasterReadinessResolver();

    private AlwaysEnabledLocalMasterReadinessResolver() {
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return Flux.just(ALWAYS_READY).concatWith(Flux.never());
    }

    public static LocalMasterReadinessResolver getInstance() {
        return INSTANCE;
    }
}
