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

package com.netflix.titus.master.supervisor.service.leader;

import java.util.Collections;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.MasterStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import rx.Observable;

@Singleton
public class ImmediateLocalMasterInstanceResolver implements LocalMasterInstanceResolver {

    private final MasterInstance ownMasterInstance;

    @Inject
    public ImmediateLocalMasterInstanceResolver(MasterDescription masterDescription) {
        this.ownMasterInstance = MasterInstance.newBuilder()
                .withInstanceId(masterDescription.getHostIP())
                .withIpAddress(masterDescription.getHostIP())
                .withStatus(MasterStatus.newBuilder()
                        .withState(MasterState.LeaderActivated)
                        .withReasonCode("none")
                        .withReasonMessage("Embedded TitusMaster activated")
                        .build()
                )
                .withStatusHistory(Collections.emptyList())
                .build();
    }

    @Override
    public Observable<MasterInstance> observeLocalMasterInstanceUpdates() {
        return Observable.just(ownMasterInstance).concatWith(Observable.never());
    }
}
