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
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.MasterStatus;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import com.netflix.titus.api.supervisor.service.MasterMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.subjects.BehaviorSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

/**
 * A {@code MasterMonitor} implementation that does not monitor anything. Use this
 * class for local testing.
 */
@Singleton
public class LocalMasterMonitor implements MasterMonitor {

    private static final Logger logger = LoggerFactory.getLogger(LocalMasterMonitor.class);

    private final MasterDescription master;

    private volatile MasterInstance ownMasterInstance;
    private volatile List<MasterInstance> masterInstances;
    private final Subject<List<MasterInstance>, List<MasterInstance>> masterUpdates = new SerializedSubject<>(
            BehaviorSubject.create(Collections.emptyList())
    );
    private final Observable<List<MasterInstance>> masterUpdatesObserver = ObservableExt.protectFromMissingExceptionHandlers(
            masterUpdates.asObservable(),
            logger
    );

    @Inject
    public LocalMasterMonitor(MasterDescription masterDescription) {
        this.master = masterDescription;
        this.ownMasterInstance = MasterInstance.newBuilder()
                .withInstanceId(masterDescription.getHostIP())
                .withInstanceGroupId("embeddedGroupId")
                .withIpAddress(masterDescription.getHostIP())
                .withStatus(MasterStatus.newBuilder()
                        .withState(MasterState.LeaderActivated)
                        .withMessage("Embedded TitusMaster activated")
                        .build()
                )
                .withStatusHistory(Collections.emptyList())
                .build();
        this.masterInstances = Collections.singletonList(ownMasterInstance);
    }


    @Override
    public Observable<MasterDescription> getLeaderObservable() {
        return Observable.just(master);
    }

    @Override
    public MasterDescription getLatestLeader() {
        return master;
    }

    @Override
    public MasterInstance getCurrentMasterInstance() {
        return ownMasterInstance;
    }

    @Override
    public Completable updateOwnMasterInstance(MasterInstance self) {
        return Completable.fromAction(() -> {
            this.masterInstances = Collections.singletonList(self);
            masterUpdates.onNext(masterInstances);
            this.ownMasterInstance = self;
        });
    }

    @Override
    public Observable<List<MasterInstance>> observeMasters() {
        return masterUpdatesObserver;
    }
}
