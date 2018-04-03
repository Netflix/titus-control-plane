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

package com.netflix.titus.master.endpoint.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.titus.api.model.ResourceDimension;

public class QueueSummary {

    private final String capacityGroup;
    private int tasksQueued;
    private int tasksLaunched;
    private double cpusQueued;
    private double cpusUsed;
    private double cpusGuarantee;
    private double memoryQueued;
    private double memoryUsed;
    private double memoryGuarantee;
    private double networkQueued;
    private double networkUsed;
    private double networkGuarantee;
    private double diskQueued;
    private double diskUsed;
    private double diskGuarantee;
    @JsonIgnore
    private boolean guaranteesSet = false;

    @JsonCreator
    public QueueSummary(@JsonProperty("capacityGroup") String capacityGroup) {
        this.capacityGroup = capacityGroup;
    }

    public void addTask(QueuableTask t, TaskQueue.TaskState state) {
        switch (state) {
            case QUEUED:
                tasksQueued++;
                cpusQueued += t.getCPUs();
                memoryQueued += t.getMemory();
                networkQueued += t.getNetworkMbps();
                diskQueued += t.getDisk();
                break;
            case LAUNCHED:
                tasksLaunched++;
                cpusUsed += t.getCPUs();
                memoryUsed += t.getMemory();
                networkUsed += t.getNetworkMbps();
                diskUsed += t.getDisk();
                break;
        }
    }

    public void setCapacityGuarantee(ResourceDimension resourceDimension, int count) {
        cpusGuarantee = resourceDimension == null ? 0.0 : resourceDimension.getCpu() * count;
        memoryGuarantee = resourceDimension == null ? 0.0 : resourceDimension.getMemoryMB() * count;
        networkGuarantee = resourceDimension == null ? 0.0 : resourceDimension.getNetworkMbs() * count;
        diskGuarantee = resourceDimension == null ? 0.0 : resourceDimension.getDiskMB() * count;
        guaranteesSet = true;
    }

    @JsonIgnore
    public boolean getGuaranteesSet() {
        return guaranteesSet;
    }

    public String getCapacityGroup() {
        return capacityGroup;
    }

    public int getTasksQueued() {
        return tasksQueued;
    }

    public int getTasksLaunched() {
        return tasksLaunched;
    }

    public double getCpusQueued() {
        return cpusQueued;
    }

    public double getCpusUsed() {
        return cpusUsed;
    }

    public double getCpusGuarantee() {
        return cpusGuarantee;
    }

    public double getMemoryQueued() {
        return memoryQueued;
    }

    public double getMemoryUsed() {
        return memoryUsed;
    }

    public double getMemoryGuarantee() {
        return memoryGuarantee;
    }

    public double getNetworkQueued() {
        return networkQueued;
    }

    public double getNetworkUsed() {
        return networkUsed;
    }

    public double getNetworkGuarantee() {
        return networkGuarantee;
    }

    public double getDiskQueued() {
        return diskQueued;
    }

    public double getDiskUsed() {
        return diskUsed;
    }

    public double getDiskGuarantee() {
        return diskGuarantee;
    }
}
