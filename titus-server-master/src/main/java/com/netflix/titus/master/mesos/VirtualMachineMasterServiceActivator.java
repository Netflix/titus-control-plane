package com.netflix.titus.master.mesos;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.VirtualMachineMasterService;
import com.netflix.titus.master.scheduler.SchedulingService;

/**
 * Helper class providing an extra indirection step for proper activation order.
 */
@Singleton
public class VirtualMachineMasterServiceActivator {

    private final VirtualMachineMasterService virtualMachineMasterService;

    @Inject
    public VirtualMachineMasterServiceActivator(VirtualMachineMasterService virtualMachineMasterService,
                                                SchedulingService schedulingService) {
        this.virtualMachineMasterService = virtualMachineMasterService;
    }

    @Activator
    public void enterActiveMode() {
        ((VirtualMachineMasterServiceMesosImpl) virtualMachineMasterService).enterActiveMode();
    }
}
