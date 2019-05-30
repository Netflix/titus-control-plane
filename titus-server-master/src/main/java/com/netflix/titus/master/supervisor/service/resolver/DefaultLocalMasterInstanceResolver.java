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

package com.netflix.titus.master.supervisor.service.resolver;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterInstanceFunctions;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.MasterStatus;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import static com.netflix.titus.api.supervisor.model.MasterInstanceFunctions.areDifferent;

@Singleton
public class DefaultLocalMasterInstanceResolver implements LocalMasterInstanceResolver {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLocalMasterInstanceResolver.class);

    private final LocalMasterReadinessResolver localMasterReadinessResolver;
    private final MasterInstance initial;

    @Inject
    public DefaultLocalMasterInstanceResolver(LocalMasterReadinessResolver localMasterReadinessResolver,
                                              MasterInstance initial) {
        this.localMasterReadinessResolver = localMasterReadinessResolver;
        this.initial = initial;
    }

    @Override
    public Flux<MasterInstance> observeLocalMasterInstanceUpdates() {
        return Flux.defer(() -> {
            AtomicReference<MasterInstance> lastRef = new AtomicReference<>(initial);
            return localMasterReadinessResolver.observeLocalMasterReadinessUpdates()
                    .flatMap(update -> {
                        Optional<MasterInstance> refreshedOpt = refresh(lastRef.get(), update);
                        if (refreshedOpt.isPresent()) {
                            MasterInstance refreshed = refreshedOpt.get();
                            lastRef.set(refreshed);
                            return Flux.just(refreshed);
                        }
                        return Flux.empty();
                    });
        });
    }

    private Optional<MasterInstance> refresh(@Nullable MasterInstance previousMasterInstance, ReadinessStatus currentReadinessStatus) {
        MasterState newState;
        switch (currentReadinessStatus.getState()) {
            case NotReady:
                newState = MasterState.Starting;
                break;
            case Disabled:
                newState = MasterState.Inactive;
                break;
            case Enabled:
                newState = MasterState.NonLeader;
                break;
            default:
                logger.warn("Unrecognized master readiness state; assuming inactive: {}", currentReadinessStatus.getState());
                newState = MasterState.Inactive;
        }

        MasterStatus newStatus = MasterStatus.newBuilder()
                .withState(newState)
                .withMessage(currentReadinessStatus.getMessage())
                .withTimestamp(currentReadinessStatus.getTimestamp())
                .build();

        if (areDifferent(previousMasterInstance.getStatus(), newStatus)) {
            MasterInstance newMasterInstance = MasterInstanceFunctions.moveTo(previousMasterInstance, newStatus);
            logger.info("MasterInstance status change: {}", newMasterInstance);
            return Optional.of(newMasterInstance);
        }

        logger.debug("Refreshed master instance status not changed: status={}", newStatus);
        return Optional.empty();
    }
}
