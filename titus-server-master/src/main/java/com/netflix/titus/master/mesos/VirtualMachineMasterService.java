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

package com.netflix.titus.master.mesos;

import java.util.List;

import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;
import rx.Observable;

public interface VirtualMachineMasterService {

    String COMPONENT = "mesos";

    void enterActiveMode();

    void launchTasks(List<Protos.TaskInfo> requests, List<VirtualMachineLease> leases);

    void rejectLease(VirtualMachineLease lease);

    void killTask(String taskId);

    void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler);

    void setRescindLeaseHandler(Action1<List<LeaseRescindedEvent>> rescindLeaseHandler);

    Observable<LeaseRescindedEvent> getLeaseRescindedObservable();

    Observable<ContainerEvent> getTaskStatusObservable();
}
