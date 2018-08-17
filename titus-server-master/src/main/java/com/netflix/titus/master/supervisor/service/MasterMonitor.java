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

package com.netflix.titus.master.supervisor.service;

import java.util.List;

import com.netflix.titus.master.supervisor.model.MasterInstance;
import rx.Completable;
import rx.Observable;

public interface MasterMonitor {

    @Deprecated
    Observable<MasterDescription> getLeaderObservable();

    @Deprecated
    MasterDescription getLatestLeader();

    /**
     * Get current {@link MasterInstance} value. This method never returns null.
     */
    MasterInstance getCurrentMasterInstance();

    /**
     * Update the local {@link MasterInstance} data record.
     */
    Completable updateOwnMasterInstance(MasterInstance self);

    /**
     * On subscribe emits information about all known TitusMaster instances. Next, emit the full list of known
     * instances whenever anything changes.
     */
    Observable<List<MasterInstance>> observeMasters();
}
