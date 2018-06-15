package com.netflix.titus.master.scheduler.constraint;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;

import static com.netflix.titus.master.scheduler.SchedulerUtils.getAttributeValueOrEmptyString;

/**
 * A constraint that checks an agent instance's state.
 */
@Singleton
public class GlobalAgentStateConstraintEvaluator implements GlobalConstraintEvaluator {

    private static final Result IN_SERVICE = new Result(true, "Instance in service");
    private static final Result QUARANTINED = new Result(false, "Instance quarantined");

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;

    @Inject
    public GlobalAgentStateConstraintEvaluator(SchedulerConfiguration schedulerConfiguration,
                                               AgentManagementService agentManagementService) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
    }

    @Override
    public String getName() {
        return "GlobalAgentStateConstraintEvaluator";
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        String instanceId = getAttributeValueOrEmptyString(targetVM, schedulerConfiguration.getInstanceAttributeName());

        // Instance does not contain all the required attributes. This should be validated when we accept the offer not here,
        // hence we let it go.
        if (StringExt.isEmpty(instanceId)) {
            return IN_SERVICE;
        }

        // TODO Should we stop it here, as the agent instance is unknown, or this check happens in another place?
        AgentInstance instance = agentManagementService.findAgentInstance(instanceId).orElse(null);
        if (instance == null) {
            return IN_SERVICE;
        }

        if (instance.getOverrideStatus().getState() == InstanceOverrideState.Quarantined) {
            return QUARANTINED;
        }

        return IN_SERVICE;
    }
}
