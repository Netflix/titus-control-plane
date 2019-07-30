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
package com.netflix.titus.ext.aws.appscale;

public class ScalableTargetResourceInfo {
    private int actualCapacity;
    private int desiredCapacity;
    private String dimensionName;
    private String resourceName;
    private String scalableTargetDimensionId;
    private String scalingStatus;
    private String version;

    public ScalableTargetResourceInfo() {}

    private ScalableTargetResourceInfo(int actualCapacity, int desiredCapacity,
                                       String dimensionName,
                                       String resourceName,
                                       String scalableTargetDimensionId,
                                       String scalingStatus, String version) {
        this.actualCapacity = actualCapacity;
        this.desiredCapacity = desiredCapacity;
        this.dimensionName = dimensionName;
        this.resourceName = resourceName;
        this.scalableTargetDimensionId = scalableTargetDimensionId;
        this.scalingStatus = scalingStatus;
        this.version = version;
    }

    public int getActualCapacity() {
        return actualCapacity;
    }

    public void setActualCapacity(int actualCapacity) {
        this.actualCapacity = actualCapacity;
    }

    public int getDesiredCapacity() {
        return desiredCapacity;
    }

    public void setDesiredCapacity(int desiredCapacity) {
        this.desiredCapacity = desiredCapacity;
    }

    public String getDimensionName() {
        return dimensionName;
    }

    public void setDimensionName(String dimensionName) {
        this.dimensionName = dimensionName;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public String getScalableTargetDimensionId() {
        return scalableTargetDimensionId;
    }

    public void setScalableTargetDimensionId(String scalableTargetDimensionId) {
        this.scalableTargetDimensionId = scalableTargetDimensionId;
    }

    public String getScalingStatus() {
        return scalingStatus;
    }

    public void setScalingStatus(String scalingStatus) {
        this.scalingStatus = scalingStatus;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "ScalableTargetResourceInfo{" +
                "actualCapacity=" + actualCapacity +
                ", desiredCapacity=" + desiredCapacity +
                ", dimensionName='" + dimensionName + '\'' +
                ", resourceName='" + resourceName + '\'' +
                ", scalableTargetDimensionId='" + scalableTargetDimensionId + '\'' +
                ", scalingStatus='" + scalingStatus + '\'' +
                ", version='" + version + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int actualCapacity;
        private int desiredCapacity;
        private String dimensionName;
        private String resourceName;
        private String scalableTargetDimensionId;
        private String scalingStatus;
        private String version;

        public Builder actualCapacity(int actualCapacity) {
            this.actualCapacity = actualCapacity;
            return this;
        }

        public Builder desiredCapacity(int desiredCapacity) {
            this.desiredCapacity = desiredCapacity;
            return this;
        }

        public Builder dimensionName(String dimensionName) {
            this.dimensionName = dimensionName;
            return this;
        }

        public Builder resourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder scalableTargetDimensionId(String scalableTargetDimensionId) {
            this.scalableTargetDimensionId = scalableTargetDimensionId;
            return this;
        }

        public Builder scalingStatus(String scalingStatus) {
            this.scalingStatus = scalingStatus;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public ScalableTargetResourceInfo build() {
            return new ScalableTargetResourceInfo(actualCapacity, desiredCapacity, dimensionName, resourceName, scalableTargetDimensionId, scalingStatus, version);
        }

    }


}


