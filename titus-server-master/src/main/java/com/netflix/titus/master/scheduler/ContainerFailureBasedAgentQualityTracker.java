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

package com.netflix.titus.master.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.TimeSeriesData;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

@Singleton
public class ContainerFailureBasedAgentQualityTracker implements AgentQualityTracker {

    private static final Logger logger = LoggerFactory.getLogger(ContainerFailureBasedAgentQualityTracker.class);

    private static final int UNDEFINED = -1;

    /*
     * Error weights. Each weight is a value between 0.1 and 0.5, or 0 if an error should be ignored.
     * This constraint is due to the accumulative effect. We do not want to overreact on a single failure.
     */

    /**
     * A container loss before it is started may indicate potential agent issue. We give it a middle score value.
     */
    private static final double LOST_BEFORE_STARTED_WEIGHT = 0.3;

    /**
     * A container loss after it was started is less likely to be an agent issue. We give it the lowest score.
     */
    private static final double LOST_AFTER_STARTED_WEIGHT = 0.1;

    /**
     * A container starting error due to agent local problems. We give it a highest score.
     */
    private static final double LOCAL_STARTING_ERROR_WEIGHT = 0.5;

    /**
     * Issue not related to agent; not counted as error.
     */
    private static final double SYSTEM_STARTING_ERROR_WEIGHT = 0;

    /**
     * A container crash may indicate underlying agent degradation. We give it a middle score value.
     */
    private static final double CONTAINER_CRASH_WEIGHT = 0.3;

    private final ConcurrentMap<String, PlacementHistory> agentsPlacementHistory = new ConcurrentHashMap<>();

    private final SchedulerConfiguration configuration;
    private final AgentManagementService agentManagementService;
    private final V3JobOperations v3JobOperations;
    private final TitusRuntime titusRuntime;
    private final CodeInvariants invariants;

    private Subscription agentStreamSubscription;
    private Subscription jobStreamSubscription;

    @Inject
    public ContainerFailureBasedAgentQualityTracker(SchedulerConfiguration configuration,
                                                    AgentManagementService agentManagementService,
                                                    V3JobOperations v3JobOperations,
                                                    TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
        this.v3JobOperations = v3JobOperations;
        this.titusRuntime = titusRuntime;
        this.invariants = titusRuntime.getCodeInvariants();
    }

    /**
     * FIXME Due to circular dependency between components, we cannot use the activation framework here.
     */
    public void start() {
        this.agentStreamSubscription = titusRuntime.persistentStream(agentManagementService.events(true)).subscribe(
                this::handleAgentEvent,
                e -> logger.error("Agent event stream terminated with an error", e),
                () -> logger.info("Agent event stream onCompleted")
        );
        this.jobStreamSubscription = titusRuntime.persistentStream(v3JobOperations.observeJobs()).subscribe(
                this::handleJobEvent,
                e -> logger.error("Job event stream terminated with an error", e),
                () -> logger.info("Job event stream onCompleted")
        );
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(agentStreamSubscription, jobStreamSubscription);
    }

    @Override
    public double qualityOf(String agentHostName) {
        PlacementHistory history = agentsPlacementHistory.get(agentHostName);
        if (history == null) {
            return UNDEFINED;
        }
        return history.quality();
    }

    private void handleAgentEvent(AgentEvent event) {
        try {
            tryHandleAgentEvent(event);
        } catch (Exception e) {
            logger.warn("Unexpected exception during handling agent event: event={}", event, e);
        }
    }

    private void tryHandleAgentEvent(AgentEvent event) {
        if (event instanceof AgentInstanceUpdateEvent) {
            AgentInstanceUpdateEvent agentEvent = (AgentInstanceUpdateEvent) event;
            String hostname = agentEvent.getAgentInstance().getHostname();
            if (agentsPlacementHistory.get(hostname) == null) {
                agentsPlacementHistory.put(hostname, new PlacementHistory(agentEvent.getAgentInstance()));
            }
        } else if (event instanceof AgentInstanceRemovedEvent) {
            String instanceId = ((AgentInstanceRemovedEvent) event).getAgentInstanceId();
            agentsPlacementHistory.values().removeIf(i -> i.getInstance().getId().equals(instanceId));
        }
    }

    private void handleJobEvent(JobManagerEvent<?> event) {
        try {
            tryHandleJobEvent(event);
        } catch (Exception e) {
            logger.warn("Unexpected exception during handling job event: event={}", event, e);
        }
    }

    private void tryHandleJobEvent(JobManagerEvent<?> event) {
        if (!(event instanceof TaskUpdateEvent)) {
            return;
        }

        TaskUpdateEvent taskEvent = (TaskUpdateEvent) event;
        Task task = taskEvent.getCurrentTask();

        if (task.getStatus().getState() == TaskState.Started) {
            handleTaskStartedEvent(task);
        } else if (task.getStatus().getState() == TaskState.Finished) {
            handleTaskFinishedEvent(task);
        }
    }

    private void handleTaskStartedEvent(Task task) {
        PlacementHistory placement = tryGetOrCreatePlacementHistory(task);
        if (placement != null) {
            placement.onTaskStarted();
        }
    }

    private void handleTaskFinishedEvent(Task task) {
        String reasonCode = task.getStatus().getReasonCode();
        if (reasonCode == null) {
            invariants.inconsistent("Task with status without reason code: taskId={}, status={}", task.getId(), task.getStatus());
            return;
        }

        ErrorKind errorKind;
        switch (reasonCode) {
            case TaskStatus.REASON_NORMAL:
            case TaskStatus.REASON_TASK_KILLED:
            case TaskStatus.REASON_FAILED:
            case TaskStatus.REASON_STUCK_IN_KILLING_STATE:
            default:
                return;
            case TaskStatus.REASON_CRASHED:
                errorKind = ErrorKind.ContainerCrash;
                break;
            case TaskStatus.REASON_TASK_LOST:
                errorKind = JobFunctions.everStarted(task) ? ErrorKind.LostAfterStarted : ErrorKind.LostBeforeStarted;
                break;
            case TaskStatus.REASON_STUCK_IN_STATE:
            case TaskStatus.REASON_LOCAL_SYSTEM_ERROR:
                errorKind = ErrorKind.LocalStartingError;
                break;
            case TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR:
            case TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR:
                errorKind = ErrorKind.SystemStartingError;
                break;
        }

        PlacementHistory placement = tryGetOrCreatePlacementHistory(task);
        if (placement != null) {
            placement.onTaskFailure(errorKind);
        }
    }

    private PlacementHistory tryGetOrCreatePlacementHistory(Task task) {
        String hostname = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
        if (hostname == null) {
            invariants.inconsistent("Task without host name assigned: taskId=%s", task.getId());
            return null;
        }

        PlacementHistory placement = agentsPlacementHistory.get(hostname);
        if (placement == null) {
            String instanceId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID);
            if (instanceId == null) {
                invariants.inconsistent("Task without agent id assigned: taskId=%s", task.getId());
                return null;
            }
            placement = agentManagementService.findAgentInstance(instanceId).map(PlacementHistory::new).orElse(null);
            if (placement != null) {
                agentsPlacementHistory.put(hostname, placement);
            }
        }

        return placement;
    }

    private enum ErrorKind {
        /**
         * A task was lost before it was started (states {@link TaskState#Accepted}, {@link TaskState#Launched}, {@link TaskState#StartInitiated}).
         */
        LostBeforeStarted,

        /**
         * A task was lost after it was start (states {@link TaskState#Started}, {@link TaskState#KillInitiated}).
         */
        LostAfterStarted,

        /**
         * A task failed to start on an agent, due to the agent's local issues.
         */
        LocalStartingError,

        /**
         * A task failed to start on an agent, due to external to the agent system issues (AWS API rate limiting, network partitioning, etc).
         */
        SystemStartingError,

        /**
         * A running container crashed due to an agent internal issues.
         */
        ContainerCrash
    }

    private class PlacementHistory {

        private final AgentInstance instance;
        private final TimeSeriesData errorTimeSeries = new TimeSeriesData(
                configuration.getContainerFailureTrackingRetentionMs(),
                1_000,
                (value, delayMs) -> {
                    long delayMin = 1 + delayMs / 60_000;
                    return value / delayMin;
                },
                titusRuntime.getClock()
        );

        private PlacementHistory(AgentInstance instance) {
            this.instance = instance;
        }

        private AgentInstance getInstance() {
            return instance;
        }

        private double quality() {
            double failureLevel = errorTimeSeries.getAggregatedValue();
            return Math.max(0, Math.min(1, 1 - failureLevel));
        }

        private void onTaskStarted() {
            errorTimeSeries.clear();
        }

        private void onTaskFailure(ErrorKind errorKind) {
            double weight;
            switch (errorKind) {
                case LostBeforeStarted:
                    weight = LOST_BEFORE_STARTED_WEIGHT;
                    break;
                case LostAfterStarted:
                    weight = LOST_AFTER_STARTED_WEIGHT;
                    break;
                case LocalStartingError:
                    weight = LOCAL_STARTING_ERROR_WEIGHT;
                    break;
                case SystemStartingError:
                    weight = SYSTEM_STARTING_ERROR_WEIGHT;
                    break;
                case ContainerCrash:
                    weight = CONTAINER_CRASH_WEIGHT;
                    break;
                default:
                    return;
            }
            errorTimeSeries.add(weight, titusRuntime.getClock().wallTime());
        }
    }
}
