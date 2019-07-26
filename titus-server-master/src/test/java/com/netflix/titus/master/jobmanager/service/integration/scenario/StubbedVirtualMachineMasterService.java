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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.LeaseRescindedEvent;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import org.apache.mesos.Protos;
import rx.Observable;
import rx.subjects.PublishSubject;

class StubbedVirtualMachineMasterService implements VirtualMachineMasterService {

    enum MesosEvent {
        TaskKillRequest
    }

    private final PublishSubject<Pair<MesosEvent, String>> eventSubject = PublishSubject.create();

    String toString(TitusExecutorDetails executorDetails) {
        try {
            return ObjectMappers.defaultMapper().writeValueAsString(executorDetails);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    TitusExecutorDetails buildExecutorDetails(String id) {
        return new TitusExecutorDetails(
                CollectionsExt.asMap("nfvpc", "1.2.3.4"),
                new TitusExecutorDetails.NetworkConfiguration(
                        true, "1.2.3.4", "1.1.1.1", "eni-12345", "eni-resource-1"
                ));
    }

    VirtualMachineLease buildLease(String taskId) {
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return "leaseOf#" + taskId;
            }

            @Override
            public long getOfferedTime() {
                return TimeUnit.DAYS.toMillis(1);
            }

            @Override
            public String hostname() {
                return "1.2.3.4";
            }

            @Override
            public String getVMID() {
                return "vm#0";
            }

            @Override
            public double cpuCores() {
                return 16;
            }

            @Override
            public double memoryMB() {
                return 16384;
            }

            @Override
            public double networkMbps() {
                return 2000;
            }

            @Override
            public double diskMB() {
                return 32768;
            }

            @Override
            public List<Range> portRanges() {
                return Collections.emptyList();
            }

            @Override
            public Protos.Offer getOffer() {
                throw new IllegalStateException("not supported");
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return Collections.emptyMap();
            }

            @Override
            public Double getScalarValue(String name) {
                return null;
            }

            @Override
            public Map<String, Double> getScalarValues() {
                return Collections.emptyMap();
            }
        };
    }

    PreferentialNamedConsumableResourceSet.ConsumeResult buildConsumeResult(String taskId) {
        return new PreferentialNamedConsumableResourceSet.ConsumeResult(0, "ENIs", "sg-12345678", 1.0);
    }

    Map<String, String> buildAttributesMap(String taskId) {
        return Collections.emptyMap();
    }

    public Observable<Pair<MesosEvent, String>> events() {
        return eventSubject.asObservable();
    }

    @Override
    public void enterActiveMode() {
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
    public void killTask(String taskId) {
        eventSubject.onNext(Pair.of(MesosEvent.TaskKillRequest, taskId));
    }

    @Override
    public void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public void setRescindLeaseHandler(Action1<List<LeaseRescindedEvent>> rescindLeaseHandler) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Observable<LeaseRescindedEvent> getLeaseRescindedObservable() {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Observable<ContainerEvent> getTaskStatusObservable() {
        return null;
    }
}
