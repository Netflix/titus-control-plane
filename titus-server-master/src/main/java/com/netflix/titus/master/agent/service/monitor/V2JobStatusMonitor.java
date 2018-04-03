/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.agent.service.monitor;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.Status;
import com.netflix.titus.master.job.worker.WorkerStateMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static com.netflix.titus.api.model.v2.V2JobState.isErrorState;
import static com.netflix.titus.api.model.v2.V2JobState.isOnSlaveState;
import static com.netflix.titus.common.util.rx.ObservableExt.mapWithState;

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

    static final String SOURCE_ID = "v2JobStatusMonitor";

    static final String HEALTHY_MESSAGE = "No task failures registered.";
    static final String UNHEALTHY_MESSAGE = "Multiple task failures registered";
    static final String TERMINATED_MESSAGE = "Agent terminated";

    private final AgentMonitorConfiguration agentMonitorConfiguration;
    private final AgentManagementService agentManagementService;
    private final WorkerStateMonitor workerStateMonitor;
    private final Registry registry;
    private final Scheduler scheduler;

    private StreamStatusMonitor delegate;

    @Inject
    public V2JobStatusMonitor(AgentMonitorConfiguration agentMonitorConfiguration,
                              AgentManagementService agentManagementService,
                              WorkerStateMonitor workerStateMonitor,
                              Registry registry) {
        this(agentMonitorConfiguration, agentManagementService, workerStateMonitor, registry, Schedulers.computation());
    }

    V2JobStatusMonitor(AgentMonitorConfiguration agentMonitorConfiguration,
                       AgentManagementService agentManagementService,
                       WorkerStateMonitor workerStateMonitor,
                       Registry registry,
                       Scheduler scheduler) {

        this.agentMonitorConfiguration = agentMonitorConfiguration;
        this.agentManagementService = agentManagementService;
        this.workerStateMonitor = workerStateMonitor;
        this.registry = registry;
        this.scheduler = scheduler;
    }

    @Activator
    public void enterActiveMode() {
        this.delegate = new StreamStatusMonitor(
                SOURCE_ID,
                false,
                agentManagementService,
                new JobStatusEvaluator(workerStateMonitor, agentMonitorConfiguration, agentManagementService, scheduler).getAgentStatusObservable(),
                registry,
                scheduler
        );
    }

    @PreDestroy
    public void shutdown() {
        if (delegate != null) {
            delegate.shutdown();
        }
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        Preconditions.checkState(delegate != null, "V2JobStatusMonitor not activated yet");
        try {
            return delegate.getStatus(agentInstanceId);
        } catch (AgentManagementException e) {
            // If no data present, assume it is healthy
            AgentInstance agentInstance = agentManagementService.getAgentInstance(agentInstanceId);
            return AgentStatus.healthy(SOURCE_ID, agentInstance, "Setting default status to healthy", scheduler.now());
        }
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return delegate.monitor();
    }

    private enum HostStatus {Ok, Bad, Undecided}

    private static class JobStatusEvaluator {

        private final Observable<AgentStatus> agentStatusObservable;
        private final AgentMonitorConfiguration config;
        private final AgentManagementService agentManagementService;
        private final Scheduler scheduler;

        private JobStatusEvaluator(WorkerStateMonitor workerStateMonitor,
                                   AgentMonitorConfiguration config,
                                   AgentManagementService agentManagementService,
                                   Scheduler scheduler) {
            this.config = config;
            this.agentManagementService = agentManagementService;
            this.scheduler = scheduler;

            this.agentStatusObservable = workerStateMonitor.getAllStatusObservable()
                    .filter(this::isActionableStatus)
                    .compose(mapWithState(new HashMap<>(), this::toAgentStatusUpdate, cleanupActions()))
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        }

        private Observable<AgentStatus> getAgentStatusObservable() {
            return agentStatusObservable;
        }

        private Observable<Function<HashMap<String, HostErrors>, Pair<Optional<AgentStatus>, HashMap<String, HostErrors>>>> cleanupActions() {
            return agentManagementService.events(true)
                    .filter(event -> event instanceof AgentInstanceRemovedEvent)
                    .map(event -> {
                        return hostErrors -> {
                            HostErrors removed = hostErrors.remove(((AgentInstanceRemovedEvent) event).getAgentInstanceId());
                            if (removed != null) {
                                AgentStatus terminatedStatus = AgentStatus.terminated(SOURCE_ID, removed.getAgentInstance(), TERMINATED_MESSAGE, scheduler.now());
                                return Pair.of(Optional.of(terminatedStatus), hostErrors);
                            }
                            return Pair.of(Optional.empty(), hostErrors);
                        };
                    });
        }

        private Pair<Optional<AgentStatus>, HashMap<String, HostErrors>> toAgentStatusUpdate(Status taskStatus, HashMap<String, HostErrors> hostErrorMap) {
            String instanceId = taskStatus.getInstanceId();
            AgentInstance instance;
            try {
                instance = agentManagementService.getAgentInstance(instanceId);
            } catch (AgentManagementException e) {
                logger.warn("Received job status update for agent {} witch is unknown to agent management subsystem", e);
                hostErrorMap.remove(instanceId);
                return Pair.of(Optional.empty(), hostErrorMap);
            }

            HostErrors hostErrors = hostErrorMap.get(instanceId);
            if (hostErrors == null) {
                hostErrorMap.put(instanceId, hostErrors = new HostErrors());
            }
            HostStatus hostStatus = hostErrors.addAndGetCurrentHostStatus(instance, taskStatus);

            logger.debug("[{}] evaluated job status={}", instanceId, hostStatus);
            switch (hostStatus) {
                case Ok:
                    AgentStatus okStatus = AgentStatus.healthy(SOURCE_ID, instance, HEALTHY_MESSAGE, scheduler.now());
                    return Pair.of(Optional.of(okStatus), hostErrorMap);
                case Bad:
                    AgentStatus badStatus = AgentStatus.unhealthy(SOURCE_ID, instance, UNHEALTHY_MESSAGE, scheduler.now());
                    return Pair.of(Optional.of(badStatus), hostErrorMap);
            }
            return Pair.of(Optional.empty(), hostErrorMap);
        }

        private boolean isActionableStatus(Status status) {
            return status.getInstanceId() != null && (isOnSlaveState(status.getState()) || isErrorState(status.getState()));
        }

        private class HostErrors {

            private final Deque<Long> errorTimestamps = new LinkedList<>();
            private AgentInstance agentInstance;

            HostStatus addAndGetCurrentHostStatus(AgentInstance instance, Status status) {
                this.agentInstance = instance;
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

            AgentInstance getAgentInstance() {
                return agentInstance;
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
}
