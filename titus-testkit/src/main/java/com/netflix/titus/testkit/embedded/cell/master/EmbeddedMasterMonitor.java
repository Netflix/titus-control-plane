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

package com.netflix.titus.testkit.embedded.cell.master;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.master.master.MasterDescription;
import com.netflix.titus.master.master.MasterMonitor;
import rx.Observable;

@Singleton
class EmbeddedMasterMonitor implements MasterMonitor {

    private final MasterDescription masterDescription;

    @Inject
    public EmbeddedMasterMonitor(MasterDescription masterDescription) {
        this.masterDescription = masterDescription;
    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return Observable.just(masterDescription).concatWith(Observable.never());
    }

    @Override
    public MasterDescription getLatestMaster() {
        return masterDescription;
    }
}
