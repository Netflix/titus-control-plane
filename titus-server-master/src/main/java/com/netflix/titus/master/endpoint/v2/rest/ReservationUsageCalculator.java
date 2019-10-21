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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.endpoint.v2.rest.representation.ReservationUsage;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;

@Singleton
public class ReservationUsageCalculator {

    private final ReadOnlyJobOperations jobOperations;
    private final ApplicationSlaManagementService capacityManagementService;

    @Inject
    public ReservationUsageCalculator(ReadOnlyJobOperations jobOperations,
                                      ApplicationSlaManagementService capacityManagementService) {
        this.jobOperations = jobOperations;
        this.capacityManagementService = capacityManagementService;
    }

    public Map<String, ReservationUsage> buildUsage() {
        Map<String, ResourceAccumulator> accumulatorMap = new HashMap<>();

        List<Pair<Job, List<Task>>> jobsAndTasks = jobOperations.getJobsAndTasks();

        Set<String> capacityGroupNames = capacityManagementService.getApplicationSLAs().stream()
                .map(ApplicationSLA::getAppName)
                .collect(Collectors.toSet());

        for (Pair<Job, List<Task>> jobAndTasks : jobsAndTasks) {
            Job job = jobAndTasks.getLeft();

            String capacityGroup = capacityGroupNames.contains(job.getJobDescriptor().getCapacityGroup())
                    ? job.getJobDescriptor().getCapacityGroup()
                    : ApplicationSlaManagementService.DEFAULT_APPLICATION;

            ResourceAccumulator accumulator = accumulatorMap.computeIfAbsent(
                    capacityGroup,
                    cp -> new ResourceAccumulator()
            );
            processJob(accumulator, jobAndTasks);
        }

        Map<String, ReservationUsage> result = new HashMap<>();
        accumulatorMap.forEach((capacityGroup, accumulator) -> result.put(capacityGroup, accumulator.toReservationUsage()));

        capacityManagementService.getApplicationSLAs().forEach(capacityGroup -> {
            if (!result.containsKey(capacityGroup.getAppName())) {
                result.put(capacityGroup.getAppName(), ReservationUsage.none());
            }
        });

        return result;
    }

    public ReservationUsage buildCapacityGroupUsage(String capacityGroupName) {
        ApplicationSLA capacityGroup = capacityManagementService.getApplicationSLA(capacityGroupName);
        if (capacityGroup == null) {
            return ReservationUsage.none();
        }

        List<Pair<Job, List<Task>>> jobsAndTasks = jobOperations.getJobsAndTasks();

        ResourceAccumulator accumulator = new ResourceAccumulator();
        for (Pair<Job, List<Task>> jobAndTasks : jobsAndTasks) {
            Job job = jobAndTasks.getLeft();
            if (capacityGroupName.equals(job.getJobDescriptor().getCapacityGroup())) {
                processJob(accumulator, jobAndTasks);
            }
        }

        return accumulator.toReservationUsage();
    }

    private void processJob(ResourceAccumulator accumulator, Pair<Job, List<Task>> jobAndTasks) {
        Job job = jobAndTasks.getLeft();
        List<Task> tasks = jobAndTasks.getRight();

        int running = 0;
        for (Task task : tasks) {
            if (TaskState.isRunning(task.getStatus().getState())) {
                running++;
            }
        }
        accumulator.add(running, job.getJobDescriptor().getContainer().getContainerResources());
    }

    private static class ResourceAccumulator {

        private double cpuSum = 0.0;
        private long memoryMbSum = 0;
        private long diskMbSum = 0;
        private long networkMbpsSum = 0;

        private void add(int count, ContainerResources containerResources) {
            if (count > 0) {
                cpuSum += count * containerResources.getCpu();
                memoryMbSum += count * containerResources.getMemoryMB();
                diskMbSum += count * containerResources.getDiskMB();
                networkMbpsSum += count * containerResources.getNetworkMbps();
            }
        }

        private ReservationUsage toReservationUsage() {
            return new ReservationUsage(cpuSum, memoryMbSum, diskMbSum, networkMbpsSum);
        }
    }
}
