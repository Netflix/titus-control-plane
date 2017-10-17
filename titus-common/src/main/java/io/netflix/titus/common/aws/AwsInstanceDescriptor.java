/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.aws;

/**
 * AWS instance descriptor.
 */
public final class AwsInstanceDescriptor {

    private String id;
    private int vCPUs;
    private int vGPUs;
    private int memoryGB;

    private int storageGB;
    private boolean ebsOnly;

    private int networkMbs;
    private int ebsBandwidthMbs;

    AwsInstanceDescriptor(Builder builder) {
        this.id = builder.id;
        this.vCPUs = builder.vCPUs;
        this.vGPUs = builder.vGPUs;
        this.memoryGB = builder.memoryGB;
        this.storageGB = builder.storageGB;
        this.ebsOnly = builder.ebsOnly;
        this.networkMbs = builder.networkMbs;
        this.ebsBandwidthMbs = builder.ebsBandwidthMbs;
    }

    public String getId() {
        return id;
    }

    public int getvCPUs() {
        return vCPUs;
    }

    public int getvGPUs() {
        return vGPUs;
    }

    public int getMemoryGB() {
        return memoryGB;
    }

    public int getStorageGB() {
        return storageGB;
    }

    public boolean isEbsOnly() {
        return ebsOnly;
    }

    public int getNetworkMbs() {
        return networkMbs;
    }

    public int getEbsBandwidthMbs() {
        return ebsBandwidthMbs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AwsInstanceDescriptor that = (AwsInstanceDescriptor) o;

        if (vCPUs != that.vCPUs) {
            return false;
        }
        if (vGPUs != that.vGPUs) {
            return false;
        }
        if (memoryGB != that.memoryGB) {
            return false;
        }
        if (storageGB != that.storageGB) {
            return false;
        }
        if (ebsOnly != that.ebsOnly) {
            return false;
        }
        if (networkMbs != that.networkMbs) {
            return false;
        }
        if (ebsBandwidthMbs != that.ebsBandwidthMbs) {
            return false;
        }
        return id != null ? id.equals(that.id) : that.id == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + vCPUs;
        result = 31 * result + vGPUs;
        result = 31 * result + memoryGB;
        result = 31 * result + storageGB;
        result = 31 * result + (ebsOnly ? 1 : 0);
        result = 31 * result + networkMbs;
        result = 31 * result + ebsBandwidthMbs;
        return result;
    }

    @Override
    public String toString() {
        return "AwsInstanceDescriptor{" +
                "id='" + id + '\'' +
                ", vCPUs=" + vCPUs +
                ", vGPUs=" + vGPUs +
                ", memoryGB=" + memoryGB +
                ", storageGB=" + storageGB +
                ", ebsOnly=" + ebsOnly +
                ", networkMbs=" + networkMbs +
                ", ebsBandwidthMbs=" + ebsBandwidthMbs +
                '}';
    }

    static Builder newBuilder(String model) {
        return new Builder(model);
    }

    static final class Builder {
        private final String id;
        private int vCPUs;
        private int vGPUs;
        private int memoryGB;

        private int storageGB;
        private boolean ebsOnly;

        private int networkMbs;
        private int ebsBandwidthMbs;

        private Builder(String id) {
            this.id = id;
        }

        public Builder cpu(int vCPUs) {
            this.vCPUs = vCPUs;
            return this;
        }

        public Builder gpu(int vGPUs) {
            this.vGPUs = vGPUs;
            return this;
        }

        public Builder memoryGB(int memoryGB) {
            this.memoryGB = memoryGB;
            return this;
        }

        public Builder networkMbs(int networkMbs) {
            this.networkMbs = networkMbs;
            return this;
        }

        public Builder storageGB(int storageGB) {
            this.storageGB = storageGB;
            return this;
        }

        public Builder ebsOnly() {
            this.ebsOnly = true;
            return this;
        }

        public Builder ebsBandwidthMbs(int ebsBandwidthMbs) {
            this.ebsBandwidthMbs = ebsBandwidthMbs;
            return this;
        }

        public AwsInstanceDescriptor build() {
            return new AwsInstanceDescriptor(this);
        }
    }
}
