package com.netflix.titus.master.service.management;

/**
 * Collection of capacity management related attributes that can be associated with agent instance groups and agent instances.
 */
public final class CapacityManagementAttributes {

    /**
     * Mark an instance group such that capacity management will ignore the instance group when doing its operations.
     */
    public static final String IGNORE = "titus.capacityManagement.ignore";
}
