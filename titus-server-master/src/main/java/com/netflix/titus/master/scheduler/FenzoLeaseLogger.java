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

import java.util.ArrayList;
import java.util.List;

import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.time.Clock;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FenzoLeaseLogger {

    /**
     * Separate log file.
     */
    private static final Logger logger = LoggerFactory.getLogger(FenzoLeaseLogger.class.getSimpleName());

    private final SchedulerConfiguration configuration;
    private final Clock clock;

    private volatile long lastLoggingTimestamp;

    FenzoLeaseLogger(SchedulerConfiguration configuration, TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.clock = titusRuntime.getClock();
    }

    void dump(List<VirtualMachineCurrentState> vmStates) {
        if (clock.isPast(lastLoggingTimestamp + configuration.getLeaseDumpIntervalMs())) {
            // Extra safeguard as this method is called in Fenzo callback.
            try {
                doDump(vmStates);
            } catch (Exception e) {
                logger.error("Lease metric update error", e);
            } finally {
                lastLoggingTimestamp = clock.wallTime();
            }
        }
    }

    private void doDump(List<VirtualMachineCurrentState> vmStates) {
        logger.info("VM leases:  vms={}", vmStates.size());
        vmStates.forEach(vm -> {
            logger.info("    host: {}", vm.getHostname());
            logger.info("        disabled until: {}", vm.getDisabledUntil() <= 0 ? "notDisabled" : DateTimeExt.toLocalDateTimeString(vm.getDisabledUntil()));
            logger.info("        available resources: cpu={}, memoryMB={}, diskMb={}, networkMbps={}, gpus={}",
                    vm.getCurrAvailableResources().cpuCores(),
                    vm.getCurrAvailableResources().memoryMB(),
                    vm.getCurrAvailableResources().diskMB(),
                    vm.getCurrAvailableResources().networkMbps(),
                    vm.getCurrAvailableResources().getScalarValues().getOrDefault("gpus", 0.0)
            );
            List<Protos.Offer> offers = new ArrayList<>(vm.getAllCurrentOffers());
            if (offers.isEmpty()) {
                logger.info("        offers: none");
            } else {
                for (int leaseIdx = 0; leaseIdx < offers.size(); leaseIdx++) {
                    logger.info("        lease[{}]: {}", leaseIdx, toResourceString(offers.get(leaseIdx)));
                }
            }
        });
    }

    private String toResourceString(Protos.Offer offer) {
        StringBuilder builder = new StringBuilder();
        for (Protos.Resource resource : offer.getResourcesList()) {
            if (resource.getType() == Protos.Value.Type.SCALAR) {
                if (builder.length() > 0) {
                    builder.append(", ");
                }
                builder.append(resource.getName()).append('=').append(resource.getScalar().getValue());
            }
        }
        return builder.toString();
    }
}
