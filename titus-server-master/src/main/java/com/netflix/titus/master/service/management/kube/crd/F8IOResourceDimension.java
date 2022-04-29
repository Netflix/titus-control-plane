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

public class F8IOResourceDimension {

    private long cpu;

    private long gpu;

    private long memoryMB;

    private long diskMB;

    private long networkMBPS;

    public long getCpu() {
        return cpu;
    }

    public void setCpu(long cpu) {
        this.cpu = cpu;
    }

    public long getGpu() {
        return gpu;
    }

    public void setGpu(long gpu) {
        this.gpu = gpu;
    }

    public long getMemoryMB() {
        return memoryMB;
    }

    public void setMemoryMB(long memoryMB) {
        this.memoryMB = memoryMB;
    }

    public long getDiskMB() {
        return diskMB;
    }

    public void setDiskMB(long diskMB) {
        this.diskMB = diskMB;
    }

    public long getNetworkMBPS() {
        return networkMBPS;
    }

    public void setNetworkMBPS(long networkMBPS) {
        this.networkMBPS = networkMBPS;
    }
}
