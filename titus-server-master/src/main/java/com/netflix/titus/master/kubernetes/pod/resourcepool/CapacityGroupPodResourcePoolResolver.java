/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import com.google.common.collect.Iterators;
import com.netflix.archaius.api.Config;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CapacityGroupPodResourcePoolResolver implements PodResourcePoolResolver {

    private static final Logger logger = LoggerFactory.getLogger(CapacityGroupPodResourcePoolResolver.class);

    private final KubePodConfiguration configuration;
    private final Config config;
    private final ApplicationSlaManagementService capacityGroupService;
    private final TitusRuntime titusRuntime;

    private final Lock lock = new ReentrantLock();
    private volatile List<Pair<String, Pattern>> resourcePoolToCapacityGroupMappers;
    private volatile long lastUpdate;

    public CapacityGroupPodResourcePoolResolver(KubePodConfiguration configuration,
                                                Config config,
                                                ApplicationSlaManagementService capacityGroupService,
                                                TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.config = config;
        this.capacityGroupService = capacityGroupService;
        this.titusRuntime = titusRuntime;
        refreshResourcePoolToCapacityGroupMappers();
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job) {
        ApplicationSLA capacityGroup = JobManagerUtil.getCapacityGroupDescriptor(job.getJobDescriptor(), capacityGroupService);
        if (capacityGroup == null) {
            return Collections.emptyList();
        }

        List<Pair<String, Pattern>> currentMappers = getCurrentMappers();
        for (Pair<String, Pattern> next : currentMappers) {
            Pattern pattern = next.getRight();
            if (pattern.matcher(capacityGroup.getAppName()).matches()) {
                return Collections.singletonList(ResourcePoolAssignment.newBuilder()
                        .withResourcePoolName(next.getLeft())
                        .withRule(String.format("Capacity group %s matches %s", capacityGroup.getAppName(), pattern.toString()))
                        .build()
                );
            }
        }

        return Collections.emptyList();
    }

    private List<Pair<String, Pattern>> getCurrentMappers() {
        if (!titusRuntime.getClock().isPast(lastUpdate + configuration.getCapacityGroupPodResourcePoolResolverUpdateIntervalMs())) {
            return resourcePoolToCapacityGroupMappers;
        }

        if (!lock.tryLock()) {
            return resourcePoolToCapacityGroupMappers;
        }

        try {
            refreshResourcePoolToCapacityGroupMappers();
        } finally {
            lock.unlock();
        }
        return resourcePoolToCapacityGroupMappers;
    }

    private void refreshResourcePoolToCapacityGroupMappers() {
        List<Pair<String, Pattern>> result = new ArrayList<>();

        String[] orderKeys = Iterators.toArray(config.getKeys(), String.class);
        Arrays.sort(orderKeys);

        for (String name : orderKeys) {
            String patternText = config.getString(name);
            try {
                result.add(Pair.of(name, Pattern.compile(patternText)));
            } catch (Exception e) {
                logger.warn("Cannot parse resource pool rule: name={}, pattern={}", name, patternText, e);
            }
        }

        this.resourcePoolToCapacityGroupMappers = result;
        this.lastUpdate = titusRuntime.getClock().wallTime();
    }
}
