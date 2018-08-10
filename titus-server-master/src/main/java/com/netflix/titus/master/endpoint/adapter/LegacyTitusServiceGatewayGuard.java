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

package com.netflix.titus.master.endpoint.adapter;

import java.util.function.Supplier;

import com.google.common.base.Strings;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.api.service.TitusServiceException.ErrorCode;
import com.netflix.titus.master.ApiOperations;
import com.netflix.titus.master.supervisor.service.LeaderActivator;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import com.netflix.titus.master.supervisor.service.MasterMonitor;
import rx.Completable;
import rx.Observable;

/**
 * An adapter implementation that guards all service calls to the legacy Titus runtime. If a node is not a Titus leader
 * returns {@link TitusServiceException} exception with {@link ErrorCode#NOT_LEADER}
 * error code. If the node is a leader, but it is not ready yet, returns an exception with status code
 * {@link ErrorCode#NOT_READY}.
 */
public class LegacyTitusServiceGatewayGuard<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE> extends
        TitusServiceGatewayAdapter<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> {

    private final ApiOperations apiOperations;
    private final MasterMonitor masterMonitor;
    private final LeaderActivator leaderActivator;

    public LegacyTitusServiceGatewayGuard(TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> delegate,
                                          ApiOperations apiOperations,
                                          MasterMonitor masterMonitor,
                                          LeaderActivator leaderActivator) {
        super(delegate);
        this.apiOperations = apiOperations;
        this.masterMonitor = masterMonitor;
        this.leaderActivator = leaderActivator;
    }

    @Override
    protected <T> Observable<T> adapt(Observable<T> replyStream) {
        return (Observable<T>) Completable.fromAction(this::checkIfActive).toObservable().concatWith(replyStream);
    }

    @Override
    protected <T> T adapt(Supplier<T> replySupplier) {
        checkIfActive();
        return replySupplier.get();
    }

    private void checkIfActive() {
        if (!leaderActivator.isLeader()) {
            MasterDescription latestMaster = masterMonitor.getLatestLeader();
            if (latestMaster != null && !Strings.isNullOrEmpty(latestMaster.getHostIP())) {
                throw TitusServiceException.notLeader(latestMaster.getHostIP());
            } else {
                throw TitusServiceException.unknownLeader();
            }
        } else if (!apiOperations.isReady()) {
            throw TitusServiceException.notReady();
        }
    }
}
