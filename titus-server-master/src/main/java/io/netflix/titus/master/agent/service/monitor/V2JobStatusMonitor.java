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

package io.netflix.titus.master.agent.service.monitor;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.job.worker.WorkerStateMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;

import static io.netflix.titus.api.model.v2.V2JobState.isErrorState;
import static io.netflix.titus.api.model.v2.V2JobState.isOnSlaveState;

/**
 * Monitor job statuses, to isolate nodes that frequently fail the submitted jobs.
 * The following rules are applied:
 * <ul>
 * <li>there must be a configurable number of consecutive errors to mark a node as unhealthy</li>
 * <li>node isolation is limited to a configurable amount of time</li>
 * <li>if errors come from different images the isolation time is exponentially increased (TODO as no image info in {@link Status})</li>
 * <li>if errors come from the same image, the isolation time is not increased</li>
 * </ul>
 * As there is no clear indication when an agent is terminated/permanently unavailable, a timeout guards an
 * observable stream for each agent.
 */
@Singleton
public class V2JobStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(V2JobStatusMonitor.class);

    static final String SOURCE_ID = "jobStatus";

    private final WorkerStateMonitor workerStateMonitor;
    private final AgentMonitorConfiguration config;
    private final AgentStatusMonitorMetrics metrics;
    private final Scheduler scheduler;
    private final AgentManagementService agentManagementService;

    @Inject
    public V2JobStatusMonitor(AgentManagementService agentManagementService,
                              WorkerStateMonitor workerStateMonitor,
                              AgentMonitorConfiguration config,
                              Registry registry) {
        this(agentManagementService, workerStateMonitor, config, registry, Schedulers.computation());
    }

    public V2JobStatusMonitor(AgentManagementService agentManagementService,
                              WorkerStateMonitor workerStateMonitor,
                              AgentMonitorConfiguration config,
                              Registry registry,
                              Scheduler scheduler) {
        this.agentManagementService = agentManagementService;
        this.workerStateMonitor = workerStateMonitor;
        this.config = config;
        this.metrics = new AgentStatusMonitorMetrics("v2JobStatusMonitor", registry);
        this.scheduler = scheduler;
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return workerStateMonitor.getAllStatusObservable()
                .filter(this::isActionableStatus)
                .groupBy(Status::getInstanceId)
                .flatMap(groupedObservable -> monitorAgent(groupedObservable, groupedObservable.getKey()));
    }

    private Observable<? extends AgentStatus> monitorAgent(GroupedObservable<String, Status> groupedObservable, String instanceId) {
        final HostErrors hostErrors = new HostErrors();

        return groupedObservable
                .timeout(config.getDeadAgentTimeout(), TimeUnit.MILLISECONDS, scheduler)
                .flatMap(status -> {
                    HostStatus hostStatus = hostErrors.addAndGetCurrentHostStatus(status);

                    AgentInstance instance;
                    try {
                        instance = agentManagementService.getAgentInstance(instanceId);
                    } catch (AgentManagementException e) {
                        logger.warn("Received job status update for agent {} witch is unknown to agent management subsystem", e);
                        return Observable.empty();
                    }

                    logger.debug("[{}] evaluated job status={}", instanceId, hostStatus);
                    switch (hostStatus) {
                        case Ok:
                            AgentStatus okStatus = AgentStatus.healthy(SOURCE_ID, instance);
                            metrics.statusChanged(okStatus);
                            return Observable.just(okStatus);
                        case Bad:
                            AgentStatus badStatus = AgentStatus.unhealthy(
                                    SOURCE_ID, instance,
                                    config.getFailingAgentIsolationTime(), scheduler.now()
                            );
                            metrics.statusChanged(badStatus);
                            return Observable.just(badStatus);
                    }
                    return Observable.empty();
                })
                .materialize()
                .map(notification -> {
                    switch (notification.getKind()) {
                        case OnNext:
                            return notification.getValue();
                        case OnError:
                            // We do not care about this error. Usually this will be timeout for an agent that was terminated.
                            if (logger.isDebugEnabled()) {
                                logger.debug("Agent {} groupedObservable terminated due to an error", notification.getThrowable());
                            }
                            break;
                        case OnCompleted:
                            if (logger.isDebugEnabled()) {
                                logger.debug("Agent {} groupedObservable completed");
                            }
                            break;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .doOnSubscribe(() -> metrics.agentAdded(instanceId))
                .doOnUnsubscribe(() -> {
                    metrics.agentRemoved(instanceId);
                    agentDisconnected(instanceId);
                });
    }

    private boolean isActionableStatus(Status status) {
        return status.getInstanceId() != null && (isOnSlaveState(status.getState()) || isErrorState(status.getState()));
    }

    protected void agentDisconnected(String hostname) {
        logger.info("Lack of activity detected from agent {}; disconnecting", hostname);
    }

    private enum HostStatus {Ok, Bad, Undecided}

    private class HostErrors {

        private final Deque<Long> errorTimestamps = new LinkedList<>();

        HostStatus addAndGetCurrentHostStatus(Status status) {
            if (status.getState() == V2JobState.Started) {
                if (!errorTimestamps.isEmpty()) {
                    errorTimestamps.clear();
                }
                return HostStatus.Ok;
            }
            long now = scheduler.now();
            if (isAgentAtFault(status)) {
                errorTimestamps.add(now);
            }
            final long checkpoint = now - config.getFailingAgentErrorCheckWindow();
            while (!errorTimestamps.isEmpty() && errorTimestamps.peekFirst() < checkpoint) {
                errorTimestamps.removeFirst();
            }
            return errorTimestamps.size() > config.getFailingAgentErrorCheckCount()
                    ? HostStatus.Bad
                    : HostStatus.Undecided;
        }

        private boolean isAgentAtFault(Status status) {
            V2JobState state = status.getState();
            if (state != V2JobState.Failed) {
                return false;
            }
            return status.getReason() != null && status.getReason() == JobCompletedReason.Lost;
        }
    }
}
