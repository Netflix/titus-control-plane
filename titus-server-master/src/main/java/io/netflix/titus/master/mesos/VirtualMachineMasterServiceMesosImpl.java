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

package io.netflix.titus.master.mesos;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.master.MasterDescription;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.zookeeper.ZookeeperPaths;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func0;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import static io.netflix.titus.common.util.LoggingExt.timed;

@Singleton
public class VirtualMachineMasterServiceMesosImpl implements VirtualMachineMasterService {

    private static final Logger logger = LoggerFactory.getLogger(VirtualMachineMasterServiceMesosImpl.class);

    private final MasterConfiguration config;
    private final SchedulerConfiguration schedulerConfiguration;
    private final JobConfiguration jobConfiguration;
    private final MasterDescription masterDescription;

    private SchedulerDriver mesosDriver;
    private MesosSchedulerCallbackHandler mesosCallbackHandler;
    private ExecutorService executor;
    private final ZookeeperPaths zkPaths;
    private MesosMasterResolver mesosMasterResolver;
    private Subject<String, String> vmLeaseRescindedObserver;
    private Subject<Status, Status> vmTaskStatusObserver;
    private ObjectMapper mapper = new ObjectMapper();
    private Func0<List<V2WorkerMetadata>> runningWorkersGetter;
    private final AtomicBoolean initializationDone = new AtomicBoolean(false);
    private final String activeAgentAttributeName;
    private final TitusTaskInfoCreator titusTaskInfoCreator;
    private double offerSecDelayInterval = 5;
    private Action1<List<? extends VirtualMachineLease>> leaseHandler = null;
    private final MesosSchedulerDriverFactory mesosDriverFactory;
    private final Registry metricsRegistry;
    private boolean active;
    private final BlockingQueue<String> killQueue = new LinkedBlockingQueue<>();

    @Inject
    public VirtualMachineMasterServiceMesosImpl(MasterConfiguration config,
                                                SchedulerConfiguration schedulerConfiguration,
                                                JobConfiguration jobConfiguration,
                                                MasterDescription masterDescription,
                                                ZookeeperPaths zkPaths,
                                                MesosMasterResolver mesosMasterResolver,
                                                MesosSchedulerDriverFactory mesosDriverFactory,
                                                Registry metricsRegistry) {
        this.config = config;
        this.schedulerConfiguration = schedulerConfiguration;
        this.jobConfiguration = jobConfiguration;
        this.masterDescription = masterDescription;
        this.zkPaths = zkPaths;
        this.mesosMasterResolver = mesosMasterResolver;
        this.mesosDriverFactory = mesosDriverFactory;
        this.metricsRegistry = metricsRegistry;
        this.vmLeaseRescindedObserver = PublishSubject.create();
        this.vmTaskStatusObserver = PublishSubject.create();
        this.titusTaskInfoCreator = new TitusTaskInfoCreator(config, jobConfiguration);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "vm_master_mesos_scheduler_thread");
            t.setDaemon(true);
            return t;
        });
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        activeAgentAttributeName = config.getActiveSlaveAttributeName();
        // Set the offer delay to match the scheduling loop interval so that offers are returned at
        // the next scheduling interval.
        offerSecDelayInterval = schedulerConfiguration.getSchedulerIterationIntervalMs() / (double) 1000;
        logger.info("Using offer second delay of " + offerSecDelayInterval);
    }

    /**
     * For healthcheck.
     */
    Optional<Boolean> isConnectedToMesos() {
        if (mesosDriver != null) {
            return Optional.of(mesosCallbackHandler.isConnected());
        }
        // Not a leader
        return Optional.empty();
    }

    // NOTE: all leases are for the same slave
    @Override
    public void launchTasks(List<Protos.TaskInfo> taskInfos, List<VirtualMachineLease> leases) {
        if (!isActivatedAndRunning()) {
            logger.error("Not in leader mode, not launching tasks");
            return;
        }
        List<Protos.OfferID> offerIDs = new ArrayList<>();
        for (VirtualMachineLease vml : leases) {
            offerIDs.add((vml).getOffer().getId());
        }
        if (!taskInfos.isEmpty()) {
            mesosDriver.launchTasks(offerIDs, taskInfos, (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(offerSecDelayInterval).build());
        } else { // reject offers to prevent offer leak, but shouldn't happen
            for (VirtualMachineLease l : leases) {
                mesosDriver.declineOffer((l).getOffer().getId());
            }
        }
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
        if (!isActivatedAndRunning()) {
            logger.error("Not in leader mode, not rejecting lease");
            return;
        }
        if (lease.getOffer() != null) {
            mesosDriver.declineOffer(lease.getOffer().getId(), (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(offerSecDelayInterval).build());
        } else {
            logger.warn("Got invalid lease to reject with null offer for host " + lease.hostname());
        }
    }

    @Override
    public void killTask(String taskId) {
        if (!isActivatedAndRunning()) {
            killQueue.offer(taskId);
            return;
        }
        drainKillTaskQueue();
        logger.info("Calling mesos to kill " + taskId);
        callMesosToKillTask(taskId);
    }

    private void drainKillTaskQueue() {
        if (killQueue.peek() != null) {
            logger.info("Carrying out pending kill requests");
            List<String> tasksToKill = new LinkedList<>();
            killQueue.drainTo(tasksToKill);
            tasksToKill.forEach(this::callMesosToKillTask);
        }
    }

    private void callMesosToKillTask(String taskId) {
        Protos.Status status = mesosDriver.killTask(
                TaskID.newBuilder()
                        .setValue(taskId)
                        .build());
        logger.info("Kill status = " + status);
        switch (status) {
            case DRIVER_ABORTED:
            case DRIVER_STOPPED:
                logger.error("Unexpected to see Mesos driver status of " + status + " from kill task request. Committing suicide!");
                System.exit(2);
        }
    }

    @Override
    public void setRunningWorkersGetter(Func0<List<V2WorkerMetadata>> runningWorkersGetter) {
        this.runningWorkersGetter = runningWorkersGetter;
    }

    @Override
    public void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler) {
        this.leaseHandler = virtualMachineLeases -> {
            drainKillTaskQueue();
            leaseHandler.call(virtualMachineLeases);
        };
    }

    @Override
    public Observable<String> getLeaseRescindedObservable() {
        return vmLeaseRescindedObserver;
    }

    @Override
    public Observable<Status> getTaskStatusObservable() {
        return vmTaskStatusObserver;
    }

    @Activator(after = SchedulingService.class)
    public void enterActiveMode() {
        logger.info("Registering Titus Framework with Mesos");

        if (!initializationDone.compareAndSet(false, true)) {
            throw new IllegalStateException("Duplicate start() call");
        }

        mesosCallbackHandler = new MesosSchedulerCallbackHandler(leaseHandler, vmLeaseRescindedObserver, vmTaskStatusObserver,
                runningWorkersGetter, config, metricsRegistry);

        FrameworkInfo framework = FrameworkInfo.newBuilder()
                .setUser("root") // Fix to root, to enable running master as non-root
                .setName(getFrameworkName())
                .setFailoverTimeout(getMesosFailoverTimeoutSecs())
                .setId(Protos.FrameworkID.newBuilder().setValue(getFrameworkName()))
                .setCheckpoint(true)
                .build();

        String mesosMaster = timed(
                "Resolving Mesos master address",
                () -> {
                    Optional<String> mm = mesosMasterResolver.resolveCanonical();
                    if (!mm.isPresent()) {
                        throw new IllegalStateException("Mesos master address not configured");
                    }
                    return mm;
                }).get();

        mesosDriver = timed(
                "Creating Mesos driver using factory " + mesosDriverFactory.getClass().getSimpleName(),
                () -> mesosDriverFactory.createDriver(framework, mesosMaster, mesosCallbackHandler)
        );

        executor.execute(() -> {
            try {
                mesosDriver.run();
            } catch (Exception e) {
                logger.error("Failed to register Titus Framework with Mesos", e);
            }
        });
        this.active = true;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Unregistering Titus Framework with Mesos");
        if (mesosDriver != null) {
            mesosDriver.stop(true);
            mesosDriver = null;
        }
        if (mesosCallbackHandler != null) {
            mesosCallbackHandler.shutdown();
        }
        executor.shutdown();
    }

    public String getFrameworkName() {
        return config.getMesosFrameworkName();
    }

    protected SchedulerDriver getMesosDriver() {
        return mesosDriver;
    }

    private double getMesosFailoverTimeoutSecs() {
        return config.getMesosFailoverTimeOutSecs();
    }

    /**
     * We need this to avoid NPE issues during shutdown, which may trigger System.exit(-3).
     * This should be properly resolved by adding de-activation mechanism.
     */
    private boolean isActivatedAndRunning() {
        return active && mesosDriver != null;
    }
}
