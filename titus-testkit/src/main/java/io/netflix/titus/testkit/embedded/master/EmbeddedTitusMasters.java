package io.netflix.titus.testkit.embedded.master;

import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.junit.master.TitusStackResource;

/**
 * A collection of preconfigured {@link EmbeddedTitusMaster} instances with different configuration tuning targets.
 */
public final class EmbeddedTitusMasters {

    /**
     * Embedded TitusMaster with configuration tuned up for faster execution, to make test fast.
     */
    public static EmbeddedTitusMaster basicMaster(SimulatedCloud simulatedCloud) {
        return EmbeddedTitusMaster.aTitusMaster()
                .withSimulatedCloud(simulatedCloud)
                .withProperty("titus.agent.cacheRefreshIntervalMs", "500")
                .withProperty("titus.agent.fullCacheRefreshIntervalMs", "500")
                .withProperty("titus.master.grpcServer.v3EnabledApps", String.format("(%s.*)", TitusStackResource.V3_ENGINE_APP_PREFIX))
                .withProperty("titus.master.capacityManagement.availableCapacityUpdateIntervalMs", "10")
                .withProperty("titus.scheduler.tierSlaUpdateIntervalMs", "10")
                .withProperty("titus.master.grpcServer.shutdownTimeoutMs", "0")
                .withProperty("titusMaster.jobManager.taskInLaunchedStateTimeoutMs", "30000")
                .withProperty("titusMaster.jobManager.batchTaskInStartInitiatedStateTimeoutMs", "30000")
                .withProperty("titusMaster.jobManager.serviceTaskInStartInitiatedStateTimeoutMs", "30000")
                .withProperty("titus.master.taskMigration.schedulerDelayInMillis", "100")
                .withProperty("titusMaster.jobManager.reconcilerIdleTimeoutMs", "100")
                .withProperty("titusMaster.jobManager.reconcilerActiveTimeoutMs", "10")
                .build();
    }
}
