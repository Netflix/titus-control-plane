/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.mesos.TitusExecutorDetails;
import org.apache.mesos.Protos;
import rx.Observable;
import rx.functions.Func0;

class StubbedVirtualMachineMasterService implements VirtualMachineMasterService {

    public String toString(TitusExecutorDetails executorDetails) {
        try {
            return ObjectMappers.defaultMapper().writeValueAsString(executorDetails);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public TitusExecutorDetails buildExecutorDetails() {
        return new TitusExecutorDetails(
                CollectionsExt.asMap("nfvpc", "1.2.3.4"),
                new TitusExecutorDetails.NetworkConfiguration(
                        true, "1.2.3.4", "1.1.1.1", "eni-12345", "eni-resource-1"
                ));
    }

    @Override
    public void launchTasks(List<Protos.TaskInfo> requests, List<VirtualMachineLease> leases) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public void setRunningWorkersGetter(Func0<List<V2WorkerMetadata>> runningWorkersGetter) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public void killTask(String taskId) {
    }

    @Override
    public void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Observable<String> getLeaseRescindedObservable() {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Observable<Status> getTaskStatusObservable() {
        return null;
    }
}
