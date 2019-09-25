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

package com.netflix.titus.master.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.runtime.TitusRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_SERVICE;

/**
 * Reports how many leases per agents are allocated to track lease fragmentation.
 */
final class FenzoLeaseMetrics {

    private static final Logger logger = LoggerFactory.getLogger(FenzoLeaseMetrics.class);

    private final static int BUCKET_COUNT = 10;

    private final Id hostLeaseCountsId;
    private final Id hostLeaseCountBucketsId;
    private final Registry registry;

    FenzoLeaseMetrics(TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.hostLeaseCountsId = registry.createId(METRIC_SCHEDULING_SERVICE + "hostLeaseCounts");
        this.hostLeaseCountBucketsId = registry.createId(METRIC_SCHEDULING_SERVICE + "hostLeaseCountBuckets");
    }

    void update(List<VirtualMachineCurrentState> vmStates) {
        // Extra safeguard as this method is called in Fenzo callback.
        try {
            doUpdate(vmStates);
        } catch (Throwable e) {
            logger.error("Lease metric update error", e);
        }
    }

    private void doUpdate(List<VirtualMachineCurrentState> vmStates) {
        Map<String, Integer> hostLeaseMap = buildHostLeaseMap(vmStates);
        int[] buckets = buildHostLeaseCountBuckets(hostLeaseMap);

        hostLeaseMap.forEach((hostname, count) ->
                registry.gauge(hostLeaseCountsId.withTag("hostname", "" + hostname)).set(count)
        );
        for (int i = 0; i < BUCKET_COUNT; i++) {
            registry.gauge(hostLeaseCountBucketsId.withTag("bucket", "" + i)).set(buckets[i]);
        }
    }

    private Map<String, Integer> buildHostLeaseMap(List<VirtualMachineCurrentState> vmStates) {
        Map<String, Integer> offerCounts = new HashMap<>();
        for (VirtualMachineCurrentState vm : vmStates) {
            offerCounts.put(vm.getHostname(), vm.getAllCurrentOffers().size());
        }
        return offerCounts;
    }

    private int[] buildHostLeaseCountBuckets(Map<String, Integer> hostLeaseMap) {
        int[] buckets = new int[BUCKET_COUNT];
        hostLeaseMap.forEach((hostname, leaseCount) -> {
            if (leaseCount >= BUCKET_COUNT) {
                buckets[BUCKET_COUNT - 1]++;
            } else {
                buckets[leaseCount]++;
            }
        });
        return buckets;
    }
}
