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

package com.netflix.titus.ext.eureka.common;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEvent;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;

/**
 * Helper class to track a collection of servers registered with Eureka with properties helping to determine their health,
 * as well as uniform load distribution. The load balancer implements round robin algorithm with bad node isolation.
 */
public class SingleServiceLoadBalancer implements Closeable, EurekaEventListener {

    private static final int INITIAL_DELAY_AFTER_FAILURE_MS = 100;

    private final Clock clock;

    private final EurekaClient eurekaClient;
    private final String vipAddress;
    private final boolean secure;

    private final AtomicReference<Map<String, EurekaInstance>> eurekaInstancesRef = new AtomicReference<>(Collections.emptyMap());

    public SingleServiceLoadBalancer(EurekaClient eurekaClient,
                                     String vipAddress,
                                     boolean secure,
                                     TitusRuntime titusRuntime) {
        this.clock = titusRuntime.getClock();
        this.eurekaClient = eurekaClient;
        this.vipAddress = vipAddress;
        this.secure = secure;
        eurekaClient.registerEventListener(this);
        refresh();
    }

    @Override
    public void close() {
        eurekaClient.unregisterEventListener(this);
    }

    @Override
    public void onEvent(EurekaEvent event) {
        if (event instanceof CacheRefreshedEvent) {
            refresh();
        }
    }

    private void refresh() {
        List<InstanceInfo> instances = eurekaClient.getInstancesByVipAddress(vipAddress, secure);
        if (CollectionsExt.isNullOrEmpty(instances)) {
            eurekaInstancesRef.set(Collections.emptyMap());
            return;
        }

        Map<String, EurekaInstance> newEurekaInstances = new HashMap<>();
        instances.forEach(instanceInfo -> {
            if (instanceInfo.getStatus() == InstanceInfo.InstanceStatus.UP) {
                String id = instanceInfo.getId();
                EurekaInstance previous = eurekaInstancesRef.get().get(id);
                if (previous == null) {
                    newEurekaInstances.put(id, new EurekaInstance(instanceInfo));
                } else {
                    previous.update(instanceInfo);
                    newEurekaInstances.put(id, previous);
                }
            }
        });
        eurekaInstancesRef.set(newEurekaInstances);
    }

    public Optional<InstanceInfo> chooseNext() {
        long now = clock.wallTime();

        Map<String, EurekaInstance> eurekaInstances = eurekaInstancesRef.get();
        if (eurekaInstances.isEmpty()) {
            return Optional.empty();
        }

        return chooseNextHealthy(eurekaInstances, now);
    }

    private Optional<InstanceInfo> chooseNextHealthy(Map<String, EurekaInstance> eurekaInstances, long now) {
        long minLastRequestTimestamp = Long.MAX_VALUE;
        EurekaInstance latestHealthy = null;

        for (EurekaInstance eurekaInstance : eurekaInstances.values()) {
            boolean disabled = eurekaInstance.getDelayUntil() > 0 && eurekaInstance.getDelayUntil() > now;
            if (!disabled && eurekaInstance.getLastRequestTimestamp() < minLastRequestTimestamp) {
                minLastRequestTimestamp = eurekaInstance.getLastRequestTimestamp();
                latestHealthy = eurekaInstance;
            }
        }

        if (latestHealthy != null) {
            latestHealthy.recordSelection(clock.wallTime());
            return Optional.of(latestHealthy.getInstanceInfo());
        }
        return Optional.empty();
    }

    public void recordSuccess(InstanceInfo instanceInfo) {
        EurekaInstance eurekaInstance = eurekaInstancesRef.get().get(instanceInfo.getId());
        if (eurekaInstance != null) {
            eurekaInstance.recordSuccess();
        }
    }

    public void recordFailure(InstanceInfo instanceInfo) {
        EurekaInstance eurekaInstance = eurekaInstancesRef.get().get(instanceInfo.getId());
        if (eurekaInstance != null) {
            eurekaInstance.recordFailure(clock.wallTime());
        }
    }

    private class EurekaInstance {

        private final AtomicReference<InstanceInfo> instanceInfoRef;

        private final AtomicLong lastRequestTimestampRef = new AtomicLong();

        private final AtomicLong delayAfterFailureRef = new AtomicLong();
        private final AtomicLong delayUntilRef = new AtomicLong();

        private EurekaInstance(InstanceInfo instanceInfo) {
            this.instanceInfoRef = new AtomicReference<>(instanceInfo);
        }

        private InstanceInfo getInstanceInfo() {
            return instanceInfoRef.get();
        }

        private long getLastRequestTimestamp() {
            return lastRequestTimestampRef.get();
        }

        private long getDelayUntil() {
            return delayUntilRef.get();
        }

        private void recordSelection(long now) {
            lastRequestTimestampRef.set(now);
        }

        private void recordSuccess() {
            delayAfterFailureRef.set(0);
            delayUntilRef.set(0);
        }

        private void recordFailure(long now) {
            delayAfterFailureRef.set(delayAfterFailureRef.get() + INITIAL_DELAY_AFTER_FAILURE_MS);
            delayUntilRef.set(now + delayAfterFailureRef.get());
        }

        private void update(InstanceInfo instanceInfo) {
            instanceInfoRef.set(instanceInfo);
        }
    }
}
