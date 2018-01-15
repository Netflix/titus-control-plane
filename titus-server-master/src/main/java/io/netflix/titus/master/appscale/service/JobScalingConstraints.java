package io.netflix.titus.master.appscale.service;

public class JobScalingConstraints {
    private int minCapacity;
    private int maxCapacity;

    public JobScalingConstraints(int minCapacity, int maxCapacity) {
        this.minCapacity = minCapacity;
        this.maxCapacity = maxCapacity;
    }

    public int getMinCapacity() {
        return minCapacity;
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

}
