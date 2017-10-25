package io.netflix.titus.ext.eureka.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

import com.google.inject.Inject;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEvent;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.agent.service.monitor.AgentMonitorUtil;
import io.netflix.titus.master.agent.service.monitor.AgentStatusMonitorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;

@Singleton
public class EurekaAgentStatusMonitor implements AgentStatusMonitor, EurekaEventListener {

    private static final Logger logger = LoggerFactory.getLogger(EurekaAgentStatusMonitor.class);

    static final String SOURCE_ID = "eureka";

    private final EurekaClient eurekaClient;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitorMetrics metrics;

    private final ConcurrentMap<String, Pair<InstanceStatus, AgentStatus>> statusByInstanceId = new ConcurrentHashMap<>();
    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();

    @Inject
    public EurekaAgentStatusMonitor(EurekaClient eurekaClient, AgentManagementService agentManagementService, Registry registry) {
        this.eurekaClient = eurekaClient;
        this.agentManagementService = agentManagementService;
        this.metrics = new AgentStatusMonitorMetrics("eurekaStatusMonitor", registry);
    }

    @Activator
    public void enterActiveMode() {
        refreshAgentDiscoveryStatus();
        eurekaClient.registerEventListener(this);
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        AgentInstance agentInstance = agentManagementService.getAgentInstance(agentInstanceId);
        Pair<InstanceStatus, AgentStatus> current = statusByInstanceId.get(agentInstance.getId());
        if (current == null) {
            // Not registered with Eureka == unhealthy
            return AgentStatus.unhealthy(SOURCE_ID, agentInstance, "Not registered with Eureka", System.currentTimeMillis());
        }
        return current.getRight();

    }

    @Override
    public Observable<AgentStatus> monitor() {
        return statusUpdateSubject.asObservable();
    }

    @Override
    public void onEvent(EurekaEvent event) {
        if (event instanceof CacheRefreshedEvent) {
            refreshAgentDiscoveryStatus();
        }
    }

    private void refreshAgentDiscoveryStatus() {
        List<String> allInstanceIds = new ArrayList<>();
        for (AgentInstanceGroup instanceGroup : agentManagementService.getInstanceGroups()) {
            for (AgentInstance instance : agentManagementService.getAgentInstances(instanceGroup.getId())) {
                allInstanceIds.add(instance.getId());
                updateInstanceStatus(instance);
            }
        }
        CollectionsExt.copyAndRemove(statusByInstanceId.keySet(), allInstanceIds).forEach(id -> {
            Pair<InstanceStatus, AgentStatus> removed = statusByInstanceId.remove(id);
            AgentStatus agentStatus = AgentStatus.terminated(SOURCE_ID, removed.getRight().getAgentInstance(), "Agent terminated", System.currentTimeMillis());
            statusUpdateSubject.onNext(agentStatus);
            metrics.statusChanged(agentStatus);
        });
    }

    private void updateInstanceStatus(AgentInstance instance) {
        List<InstanceInfo> instancesById = eurekaClient.getInstancesById(instance.getId());
        Optional<InstanceInfo> instanceInfo = instancesById.isEmpty() ? Optional.empty() : Optional.of(instancesById.get(0));
        InstanceInfo.InstanceStatus newStatus = instanceInfo.map(InstanceInfo::getStatus).orElse(InstanceStatus.UNKNOWN);

        Pair<InstanceStatus, AgentStatus> previous = statusByInstanceId.get(instance.getId());
        if (previous == null) {
            updateAndEmit(instance, instanceInfo, newStatus);
        } else {
            if (newStatus != previous.getLeft()) {
                updateAndEmit(instance, instanceInfo, newStatus);
            }
        }
    }

    private void updateAndEmit(AgentInstance instance, Optional<InstanceInfo> instanceInfo, InstanceStatus instanceStatus) {
        String reason = instanceInfo.map(i -> {
            String myReason;
            if (i.getMetadata() != null && (myReason = i.getMetadata().get("RegistrationStatus.reason")) != null) {
                return myReason;
            }
            return "no details";
        }).orElse("no details");

        AgentStatus agentStatus;
        switch (instanceStatus) {
            case UP:
                agentStatus = AgentStatus.healthy(SOURCE_ID, instance, "Agent is UP in Eureka (" + reason + ")", System.currentTimeMillis());
                break;
            case STARTING:
            case DOWN:
            case OUT_OF_SERVICE:
                agentStatus = AgentStatus.unhealthy(SOURCE_ID, instance, "Agent is " + instanceStatus + " in Eureka (" + reason + ")", System.currentTimeMillis());
                break;
            case UNKNOWN:
            default:
                agentStatus = AgentStatus.unhealthy(SOURCE_ID, instance, "Agent not registered with Eureka (" + reason + ")", System.currentTimeMillis());
        }
        statusByInstanceId.put(instance.getId(), Pair.of(instanceStatus, agentStatus));
        statusUpdateSubject.onNext(agentStatus);
        logger.info(AgentMonitorUtil.toStatusUpdateSummary(agentStatus));
        metrics.statusChanged(agentStatus);
    }
}
