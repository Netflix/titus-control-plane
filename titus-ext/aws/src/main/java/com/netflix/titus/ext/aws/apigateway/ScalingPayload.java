package com.netflix.titus.ext.aws.apigateway;

public class ScalingPayload {
    private int actualCapacity;
    private int desiredCapacity;
    private String dimensionName;
    private String resourceName;
    private String scalableTargetDimensionId;
    private String scalingStatus;
    private String version;

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
        return "ScalingPayload{" +
                "actualCapacity=" + actualCapacity +
                ", desiredCapacity=" + desiredCapacity +
                ", dimensionName='" + dimensionName + '\'' +
                ", resourceName='" + resourceName + '\'' +
                ", scalableTargetDimensionId='" + scalableTargetDimensionId + '\'' +
                ", scalingStatus='" + scalingStatus + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}


