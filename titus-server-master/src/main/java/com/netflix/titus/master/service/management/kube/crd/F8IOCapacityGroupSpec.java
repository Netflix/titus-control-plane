/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.master.service.management.kube.crd;

public class F8IOCapacityGroupSpec {

    private String resourcePoolName;

    private String createdBy;

    private String schedulerName;

    private String capacityGroupName;

    private int instanceCount;

    private F8IOResourceDimension resourceDimensions;

    public String getResourcePoolName() {
        return resourcePoolName;
    }

    public void setResourcePoolName(String resourcePoolName) {
        this.resourcePoolName = resourcePoolName;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    public void setSchedulerName(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    public String getCapacityGroupName() {
        return capacityGroupName;
    }

    public void setCapacityGroupName(String capacityGroupName) {
        this.capacityGroupName = capacityGroupName;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount = instanceCount;
    }

    public F8IOResourceDimension getResourceDimensions() {
        return resourceDimensions;
    }

    public void setResourceDimensions(F8IOResourceDimension resourceDimensions) {
        this.resourceDimensions = resourceDimensions;
    }
}
