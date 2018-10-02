package com.netflix.titus.master.agent.service.monitor;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.common.runtime.TitusRuntime;
import rx.Observable;

@Singleton
public class LifecycleAgentStatusMonitor implements AgentStatusMonitor {

    static final String SOURCE_ID = "lifecycle";

    private final AgentManagementService agentManagementService;
    private final TitusRuntime titusRuntime;
    private final ConcurrentMap<String, AgentInstance> knownInstances = new ConcurrentHashMap<>();

    @Inject
    public LifecycleAgentStatusMonitor(AgentManagementService agentManagementService, TitusRuntime titusRuntime) {
        this.agentManagementService = agentManagementService;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        return findStatus(agentInstanceId).orElseThrow(() -> AgentManagementException.agentNotFound(agentInstanceId));
    }

    @Override
    public boolean isHealthy(String agentInstanceId) {
        return findStatus(agentInstanceId).map(s -> s.getStatusCode() == AgentStatus.AgentStatusCode.Healthy).orElse(false);
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return agentManagementService.events(false).flatMap(event -> {
            if (event instanceof AgentInstanceUpdateEvent) {
                AgentInstance agentInstance = ((AgentInstanceUpdateEvent) event).getAgentInstance();
                knownInstances.put(agentInstance.getId(), agentInstance);
                return Observable.just(statusOf(agentInstance));
            }
            if (event instanceof AgentInstanceRemovedEvent) {
                // As AgentInstanceRemovedEvent does not contain AgentInstance value, which is required in AgentStatus
                // we have to cache it, and tweak when the AgentInstanceRemovedEvent event is received.
                AgentInstance agentInstance = knownInstances.remove(((AgentInstanceRemovedEvent) event).getAgentInstanceId());
                if (agentInstance != null) {
                    return Observable.just(toTerminatedEvent(agentInstance));
                }
            }
            return Observable.empty();
        });
    }

    private Optional<AgentStatus> findStatus(String agentInstanceId) {
        return agentManagementService.findAgentInstance(agentInstanceId).map(this::statusOf);
    }

    private AgentStatus statusOf(AgentInstance agent) {
        InstanceLifecycleState state = agent.getLifecycleStatus().getState();
        if (state == InstanceLifecycleState.Started) {
            return AgentStatus.healthy(SOURCE_ID, agent, "Agent in Started state", titusRuntime.getClock().wallTime());
        }
        return AgentStatus.unhealthy(SOURCE_ID, agent, "Agent not in Started state: currentState=" + state, titusRuntime.getClock().wallTime());
    }

    private AgentStatus toTerminatedEvent(AgentInstance agentInstance) {
        AgentInstance terminatedInstance = agentInstance.toBuilder().withDeploymentStatus(InstanceLifecycleStatus.newBuilder()
                .withState(InstanceLifecycleState.Stopped)
                .withLaunchTimestamp(agentInstance.getLifecycleStatus().getLaunchTimestamp())
                .build()
        ).build();
        return AgentStatus.terminated(agentInstance.getId(), terminatedInstance, "Agent instance terminated", titusRuntime.getClock().wallTime());
    }
}
