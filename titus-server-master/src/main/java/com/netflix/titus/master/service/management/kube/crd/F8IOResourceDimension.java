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

    private int cpu;

    private int gpu;

    private int memoryMB;

    private int diskMB;

    private int networkMBPS;

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public int getGpu() {
        return gpu;
    }

    public void setGpu(int gpu) {
        this.gpu = gpu;
    }

    public int getMemoryMB() {
        return memoryMB;
    }

    public void setMemoryMB(int memoryMB) {
        this.memoryMB = memoryMB;
    }

    public int getDiskMB() {
        return diskMB;
    }

    public void setDiskMB(int diskMB) {
        this.diskMB = diskMB;
    }

    public int getNetworkMBPS() {
        return networkMBPS;
    }

    public void setNetworkMBPS(int networkMBPS) {
        this.networkMBPS = networkMBPS;
    }
}
