/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.mesos;

import java.util.List;

import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import rx.Observable;

public class NoOpVirtualMachineMasterService implements VirtualMachineMasterService {
    @Override
    public void enterActiveMode() {
    }

    @Override
    public void launchTasks(TaskAssignments taskAssignments) {
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
    }

    @Override
    public void killTask(String taskId) {
    }

    @Override
    public void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler) {
    }

    @Override
    public void setRescindLeaseHandler(Action1<List<LeaseRescindedEvent>> rescindLeaseHandler) {
    }

    @Override
    public Observable<LeaseRescindedEvent> getLeaseRescindedObservable() {
        return Observable.never();
    }

    @Override
    public Observable<ContainerEvent> getTaskStatusObservable() {
        return Observable.never();
    }
}
