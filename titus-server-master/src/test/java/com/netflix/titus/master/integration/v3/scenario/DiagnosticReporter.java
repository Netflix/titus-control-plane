package com.netflix.titus.master.integration.v3.scenario;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.scheduler.SchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingResultEvent.FailedSchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingResultEvent.SuccessfulSchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to dump TitusMaster internal state in case a test fails for diagnostic purposes.
 */
public class DiagnosticReporter {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticReporter.class);

    private final AgentManagementService agentManagement;
    private final V3JobOperations jobOperations;
    private final SchedulingService schedulingService;
    private final SimulatedCloud simulatedCloud;

    public DiagnosticReporter(EmbeddedTitusMaster titusMaster) {
        this.simulatedCloud = titusMaster.getSimulatedCloud();
        this.agentManagement = titusMaster.getInstance(AgentManagementService.class);
        this.jobOperations = titusMaster.getInstance(V3JobOperations.class);
        this.schedulingService = titusMaster.getInstance(SchedulingService.class);
    }

    public void reportAgentsInTheCloud() {
        logger.info("Reporting all agent instances running in the cloud:");
        for (SimulatedTitusAgentCluster instanceGroup : simulatedCloud.getAgentInstanceGroups()) {
            logger.info("Simulated agent instance group: id={}", instanceGroup.getName());
            for (SimulatedTitusAgent agent : instanceGroup.getAgents()) {
                logger.info("    {}: hostname={}", agent.getId(), agent.getHostName());
            }
        }
    }

    public void reportAllAgentsWithAssignments() {
        logger.info("Reporting all agent known to Titus:");
        for (AgentInstanceGroup instanceGroup : agentManagement.getInstanceGroups()) {
            logger.info("Agent instance group: id={}, tier={}, state={}", instanceGroup.getId(), instanceGroup.getTier(), instanceGroup.getLifecycleStatus());
            for (AgentInstance instance : agentManagement.getAgentInstances(instanceGroup.getId())) {
                logger.info("    {}: status={}, tasks={}", instance.getId(), instance.getLifecycleStatus(), getRunningTasksOn(instance));
            }
        }
    }

    public void reportWhenTaskNotScheduled(String taskId) {
        Pair<Job<?>, Task> jobAndTask = jobOperations.findTaskById(taskId).orElseThrow(() -> new IllegalStateException("Task not found: " + taskId));
        Task task = jobAndTask.getRight();

        Optional<SchedulingResultEvent> resultOpt = schedulingService.findLastSchedulingResult(task.getId());
        if (resultOpt.isPresent()) {
            SchedulingResultEvent result = resultOpt.get();
            if (result instanceof SuccessfulSchedulingResultEvent) {
                logger.info("Task successfully scheduled: {}", result);
            } else {
                FailedSchedulingResultEvent failure = (FailedSchedulingResultEvent) result;
                logger.info("Task scheduling failure: {}", failure);
                reportAllAgentsWithAssignments();
            }
        } else {
            logger.info("Task not found in the scheduler: {}", task.getId());
        }
    }

    private List<String> getRunningTasksOn(AgentInstance instance) {
        return jobOperations.getTasks().stream()
                .filter(task -> runsOnAgent(task, instance))
                .map(Task::getId)
                .collect(Collectors.toList());
    }

    private boolean runsOnAgent(Task task, AgentInstance instance) {
        String taskAgentId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID);
        return instance.getId().equals(taskAgentId);
    }
}
